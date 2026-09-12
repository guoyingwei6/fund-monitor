# 盘中基金提醒 GitHub Pages

纯静态页面，不需要服务器、Cloudflare Worker 或访问令牌。

## 功能

- 纯客户端运行：东方财富移动端盘中估值公开接口（支持 CORS），无需后端服务。
- 预设 Notion 组合中的 4 只基金（华夏纯债、易方达A500联接、南方纳斯达克100QDII、易方达港股通红利低波）。
- 涨超 / 跌超阈值双向刻度展示与触发提醒，支持同一方向每个交易日仅提醒一次。
- 配置完全保存在浏览器本地 `localStorage`，避免将个人 Bark 推送 Key 泄露到公共仓库。
- 支持通过 URL Hash 快速导入 Bark 地址，例如：`#bark=https://bark.guoyingwei.top/your-key/Body`。

## 使用

1. 直接在浏览器打开 `index.html` 或通过 GitHub Pages 访问。
2. 首次进入可通过点击右上角齿轮图标配置 Bark 地址，或通过 `#bark=https://...` 链接一键导入。
3. 根据需要微调各基金的涨跌提醒阈值（百分比）。
4. 点击右上角「开始监控」并保持页面打开即可。

## 部署

仓库中已配置 `.github/workflows/fund-alert-pages.yml`，且仓库已开启 GitHub Actions Pages 构建。推送到 `main` 分支后，GitHub Actions 会自动部署至 `https://guoyingwei6.github.io/fund-monitor/`。

## 数据与限制

估值来自天天基金移动端公开接口。它是估算值，可能与最终确认净值有偏差。收盘后估算字段为空，页面显示 `-`。由于是纯静态页面，Bark 提醒依赖浏览器页面保持打开；后台标签页可能被浏览器节流，不适合作为无人值守的强实时提醒。
