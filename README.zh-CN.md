# nanoprotocol

每个服务编译时都要对上的线上契约：消息类型、信封、客户端，以及权限匹配。

它是一个 **Go 模块，不是服务** —— 没有端口、没有数据库、没有副作用。

## 它是什么

- **81 个消息类型常量**，按它们跨越的边界分组：网关 ↔ 总线、智能体 ↔ 总线、容器管理、
  身份、流式、智能体 socket、隧道与终端流。
- **一个信封。** `Message` 带类型、负载与路由字段；每个请求都有对应的响应类型。
- **两个客户端。** 带重连与 RPC 风格调用的 WebSocket 客户端，以及 REST / SSE 面上的 HTTP 客户端。
- **必须一致、不能各写一套的语义。** `MediaItem`、`ConfigItem`、会话信封，以及
  `WildcardMatch` —— 权限模式只有这一份实现，所有服务判定「这个工具能不能跑」都用它。
- **不含业务逻辑。** 不碰数据库、不调模型、不带聊天 SDK、不读配置。

## 它在哪

| | |
|---|---|
| 形态 | 库 —— 被十个模块 import |
| 版本 | 发布里是 `0.46.0`；由超仓的 `versions.lock` 钉住 |
| 依赖 | 只有 `github.com/gorilla/websocket` 与标准库 |
| 使用方 | `nanobus`、`nanogateway`、`nanoagent`、`nanoportal`、`nanoauth`、`nanoadmin`、`nanoprovisioner`、`nanoartifacts`、`nanoworkspace`、`nanoui` |

## 快速开始

```bash
go get gitee.com/kobeyoung81/nanoprotocol@v0.46.0
```

```go
c := nanoprotocol.NewClient(busURL)
msg := nanoprotocol.Message{Type: nanoprotocol.TypePollInbound, UserID: userID}
resp, err := c.Request(ctx, msg)
if err == nil && resp.Type == nanoprotocol.TypeInbound {
    // resp.Inbound 里就是那条消息
}
```

在工作区里这个模块通过 `go.work` 使用；使用方从不 vendor 它，所以协议改动只有一个 tag。

## 消息分组

| 组 | 边界 | 说明 |
|---|---|---|
| Gateway | 网关 ↔ 总线 | 解析平台身份、投递入站、轮询出站 |
| Agent | 智能体 ↔ 总线 | 轮询入站、投递出站、流式分片与结束 |
| Container | runner ↔ 总线 | 请求、列表、轮询待处理、更新状态、销毁 |
| Auth | 服务 ↔ 总线 ↔ nanoauth | 登录、注册、校验令牌 |
| Config | 服务 ↔ 总线 | 智能体维度的配置与变更事件 |
| Users | 任意 ↔ 总线 | 更新、状态、信息 |
| Sessions | 智能体 / 控制台 ↔ 总线 | 保存、归档、列表、取详情 |
| Tunnel | hypervisor ↔ 总线 | 注册、请求、响应 |
| Terminal | 浏览器 ↔ 总线 ↔ hypervisor | attach、数据、关闭 |
| Agent socket | 智能体 ↔ 总线（WS） | 与 `/api/v1/rpc` 同一套调用，外加两个无状态推送 |

**智能体管理不在协议里。** 建、改、列、删智能体都是打给门户的 HTTP 调用，行归门户；
总线上只剩一条变更通知，给监督者订阅。

## 设计决策

- **类型是字符串，不是数字。** 日志与 SSE 帧可读，未知类型可以被报出来，而不是被悄悄解错。
- **只有一个依赖。** `gorilla/websocket` —— 再多一个，就会把某个传输或框架漏进每个服务。
- **权限匹配放在这里。** 每个模块都要用同一种方式回答「这个工具能不能跑」，
  只留一份实现是唯一能一直保持一致的办法。
- **信封稳定，负载长大。** 新功能加字段、加类型；它们不改变使用方已经在解的形状。
- **运行时不做版本协商。** 协议不匹配在构建时就暴露，所以每个使用方钉同一个 tag。

## 改协议

1. 先在这里提交改动。
2. 在超仓跑 `./scripts/bump-protocol.sh vX.Y.Z` —— 它给本模块打 tag，并更新每个使用方的 `go.mod`。
3. 在每个使用方提交升过的 `go.mod` / `go.sum`。
4. **先推子模块、再推超仓**，这样任何一次检出都不会指向远端还没有的 tag。

## 文档

- [`docs/ARCHITECTURE.md`](https://gitee.com/kobeyoung81/nanosec/blob/master/docs/ARCHITECTURE.md) —— 各服务如何使用这份契约
- [`docs/README.md`](https://gitee.com/kobeyoung81/nanosec/blob/master/docs/README.md) —— 全部文档索引

## 状态

以 `v0.46.0` 发布；版本钉在超仓的 `versions.lock` 里。

## 贡献与安全

开发与提交规则见
[`CONTRIBUTING.md`](https://gitee.com/kobeyoung81/nanosec/blob/master/CONTRIBUTING.md)；
漏洞请按
[`SECURITY.md`](https://gitee.com/kobeyoung81/nanosec/blob/master/SECURITY.md) 私下报告，
不要开公开 issue。

## 许可证

[AGPL-3.0](./LICENSE)。名称与标志另有说明 —— 见 [`NOTICE`](./NOTICE)。
