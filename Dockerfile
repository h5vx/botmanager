# syntax=docker/dockerfile:1
#
# Multi-stage: статический бинарь без CGO, финальный образ — distroless
# (никакой оболочки, пакетного менеджера или libc с известными CVE внутри).
#
# Билдер закреплён на go 1.25, потому что именно это требует go.mod
# (сработала MVS-минимальная версия транзитивных зависимостей grpc и
# prometheus/client_golang на момент написания — см. CLAUDE.md,
# раздел "Версия Go").

FROM golang:1.25-alpine AS builder

WORKDIR /src

# api/ — отдельный модуль (контракт + клиент), на который основной модуль
# ссылается через replace; без него go mod download не разрешит зависимость.
COPY go.mod go.sum ./
COPY api/go.mod api/go.sum ./api/
RUN go mod download

COPY . .

# Права фиксируются здесь явно (в builder есть shell), а не через COPY
# --chmod в финальной stage: --chmod применяется рекурсивно и к
# автоматически создаваемым родительским директориям, поэтому
# `--chmod=644` на config/config.yaml делает НЕПРОХОДИМЫМ (без +x) сам
# каталог /app/config — nonroot не может его открыть. Правильно —
# директории 755 (traverse+list), файлу 644 (read) достаточно.
RUN chmod 755 config && chmod 644 config/*.yaml

# node.data_dir (config/config.yaml: /var/lib/botmanager по умолчанию) —
# каталог BoltDB-журнала Raft и снимков (internal/raftcluster), которого в
# пустом distroless-финале нет и который raftcluster.Open создаёт сам
# (os.MkdirAll) только если он уже доступен для записи — под non-root это
# не так для каталога, которого вообще нет. Создаём его здесь же (тот же
# приём, что и для config/ чуть выше — права выставляются, пока в стадии
# есть shell) и копируем с --chown в финальный образ, тем же способом ниже.
RUN mkdir -p /out/data && chmod 755 /out/data

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/botmanager ./cmd/botmanager

FROM gcr.io/distroless/static-debian12:nonroot AS final

WORKDIR /app
# --chown обязателен: без него файлы остаются root:root (владелец сборки),
# а под пользователем nonroot ниже это тоже дало бы "permission denied" —
# независимо от режима доступа. Здесь --chmod намеренно НЕ указываем: режимы
# уже корректно выставлены выше (755/644), COPY --from без --chmod их
# сохраняет как есть. 65532:65532 — фиксированный uid/gid nonroot в
# gcr.io/distroless/*:nonroot, задан числом, чтобы не зависеть от записи в
# /etc/passwd на этапе COPY.
COPY --from=builder --chown=65532:65532 --chmod=755 /out/botmanager /app/botmanager
COPY --from=builder --chown=65532:65532 /src/config /app/config
# /var/lib/botmanager — node.data_dir по умолчанию (config/config.yaml).
# Копируем уже готовый (755, см. builder-стадию выше) пустой каталог с
# правильным владельцем вместо RUN mkdir здесь — в distroless-финале нет ни
# shell, ни mkdir.
COPY --from=builder --chown=65532:65532 /out/data /var/lib/botmanager

# 9090 — gRPC API, 9091 — /healthz /readyz /metrics, 9092 — Raft.
# Сертификаты и ключ токенов ожидаются в /etc/botmanager/certs (см.
# security.* в config/config.yaml) — смонтируйте их томом или секретом.
EXPOSE 9090 9091 9092

USER nonroot:nonroot

ENTRYPOINT ["/app/botmanager"]
CMD ["-config", "/app/config/config.yaml"]
