# 自定义编译

在 GitHub Actions 选择 **Custom Build → Run workflow**。工作流只安装依赖、编译和上传产物，不运行测试。

| 输入 | 用途 | 留空或默认值 |
| --- | --- | --- |
| `version` | 后端产物版本号，可带 `v` | 从 `OpenListTeam/OpenList` 获取最新正式版号 |
| `frontend_mode` | `source` 编译源码；`release` 下载前端发行包 | `source` |
| `frontend_repo` | 前端仓库 | `12189108/OpenList-Frontend` |
| `frontend_ref` | 源码模式的分支、标签或 SHA | `main` |
| `frontend_version` | 源码模式的版本标签，或 Release 模式的发行标签 | 源码模式取 `OpenListTeam/OpenList-Frontend` 最新正式版号；Release 模式取所选仓库最新正式版 |
| `publish` | 编译后同时发布 GitHub Release | 关闭，只上传 Actions Artifact |

**自动获取的是版本号。后端始终编译 Run workflow 所选分支，源码模式的前端始终编译 `frontend_ref`，不会切回官方发行版源码。** 要编译合并后的前端，请先将前端合并分支推送到自己的仓库，再把 `frontend_ref` 设为该分支；合并进 main 后使用默认值即可。

产物包含 Linux/Windows 的 amd64、arm64 二进制及 `build-info.json`，记录版本、前后端提交和前端来源。开启发布时，若仓库已有同名标签，使用新的自定义版本号，例如 `v4.2.6-custom.1`。

## 独立脚本

在 Linux/WSL 下运行，需要 Bash、GitHub CLI（已登录或设置 `GH_TOKEN`）、jq、git、Go、tar。源码模式还需要 Node 和前端 `packageManager` 对应的 pnpm。

```bash
# 自动版本，编译自己前端 main 的最新源码
bash scripts/build-custom.sh

# 指定版本、前端分支
bash scripts/build-custom.sh --version v4.2.6-custom.1 \
  --frontend-version v4.2.6-custom.1 --frontend-ref feature/my-upload

# 下载自己仓库的最新正式前端 Release
bash scripts/build-custom.sh --frontend-mode release

# 使用指定前端发行包
bash scripts/build-custom.sh --version v4.2.6-custom.1 \
  --frontend-mode release --frontend-version v4.2.0

# 使用本地前端工作区，仅编译 Linux amd64
bash scripts/build-custom.sh --frontend-source-dir /path/to/OpenList-Frontend \
  --targets "linux/amd64"
```

版本也可写 `auto`。`--version-repo` 和 `--frontend-version-repo` 可调整自动查询版本号的仓库。已有 `--frontend-source-dir` 按原状编译，不自动拉取或切换分支；未指定时，每次拉取 `frontend_ref` 到临时目录。前端构建期间临时更新 package.json 的版本，结束后恢复原文件。

四平台交叉编译需要 `gcc`、`g++`、`aarch64-linux-gnu-gcc/g++`、`x86_64-w64-mingw32-gcc/g++`，以及 PATH 中的 Zig 0.11.0。Windows arm64 使用仓库的 `wrapper/zcc-arm64`、`wrapper/zcxx-arm64`（需可执行权限）。Actions 自动安装这些工具；本地可用 `--targets` 只选已安装工具链的目标。默认输出目录为 `build`。
