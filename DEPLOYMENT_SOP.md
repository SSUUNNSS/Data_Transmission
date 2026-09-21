# Data Transmission Ingrid 部署 SOP

本文档适用于 Windows Server + PowerShell 环境。

项目目录示例：

```text
C:\Data_Transmission_Ingrid
```

## 1. 服务器准备

安装 Python 3.10 或更高版本，并确认：

```powershell
python --version
```

项目依赖包括 `pyarrow`、`paho-mqtt`、`watchdog`，详见 `requirements.txt`。

## 2. 上传项目

上传以下目录和文件：

```text
config/
src/
state/
requirements.txt
```

以下内容可以不上传：

```text
.venv/
logs/
outputs/mqtt_preview/
```

如果需要继续之前的发送进度，必须保留对应的 `state/*.json` 文件。

## 3. 创建虚拟环境并安装依赖

```powershell
cd C:\Data_Transmission_Ingrid
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

验证依赖：

```powershell
python -c "import pyarrow, paho.mqtt, watchdog; print('dependencies ok')"
```

## 4. 配置电站和 MQTT

每个电站对应一个配置文件，例如：

```text
config/karlskrona_local.json
config/falkoping_local.json
```

确认以下字段使用服务器上的真实值：

```json
{
  "message": {
    "ps_id": 21,
    "app_key": "真实app_key",
    "source": "ubuntu-server",
    "data_type_mapping_path": "config/data_type_mapping.json"
  },
  "mqtt": {
    "username": "真实用户名",
    "password": "真实密码",
    "client_id": "真实client_id"
  }
}
```

上线前检查：

- `ps_id`
- `app_key`
- MQTT 用户名和密码
- `encrypted`、`encrypted_port`、`plain_port`
- MQTT topic
- `data_type_mapping_path`
- `source.directory`

配置文件包含 MQTT 密码，应限制文件权限，不要提交到 Git 或公开传输。示例：

```powershell
icacls config\*.json /inheritance:r
icacls config\*.json /grant:r "$env:USERNAME:(R)"
```

## 5. 准备 Parquet 数据目录

实时和历史数据目录必须使用以下结构：

```text
src/sourceData/
├── normal/
│   └── Karlskrona/
│       └── 2026-09-21/
│           └── data.parquet
└── history_data/
    └── Karlskrona/
        └── 2026-09-21/
            └── data.parquet
```

要求：

- 文件扩展名必须为 `.parquet`
- 日期目录必须为 `YYYY-MM-DD`
- watcher 路径必须是 `normal/{StationName}/{YYYY-MM-DD}/{file}.parquet` 或 `history_data/{StationName}/{YYYY-MM-DD}/{file}.parquet`
- Parquet 必须包含 `ts_utc`、`metric`、`value` 三列
- 站点名必须和 `src/watch_folder.py` 的 `STATIONS` 注册名称一致

## 6. 注册新电站

如果使用自动监控，编辑 `src/watch_folder.py` 的 `STATIONS`：

```python
"NewStation": PROJECT_ROOT / "config" / "newstation_local.json",
```

同时创建：

```text
config/newstation_local.json
src/sourceData/normal/NewStation/
src/sourceData/history_data/NewStation/
```

状态文件会自动生成：

```text
state/newstation_processed_files.json
```

不要复制其他电站的状态文件作为新电站状态。

## 7. 部署后测试

```powershell
.\.venv\Scripts\Activate.ps1
python -m unittest discover -s tests -v
```

检查配置能否加载：

```powershell
python -c "import sys; sys.path.insert(0, 'src'); from pathlib import Path; from transmission import load_runtime_config; load_runtime_config(Path('config/karlskrona_local.json')); print('config ok')"
```

## 8. 预览验证

预览模式只读取和构建消息，不连接 MQTT：

```powershell
python src\main.py `
  --config config\karlskrona_local.json `
  --send-mode history_data `
  --preview `
  --preview-limit 20
```

预览文件默认位于：

```text
outputs/mqtt_preview/messages_preview.jsonl
```

重点检查：

- `psId`
- `appkey`
- `topic`
- `timestamp`
- `devChar`
- `data`
- `data_type`
- `payload_bytes`

当前程序不会自动把数据筛选或聚合成 5 分钟粒度，时间戳按原始 Parquet 数据处理。

## 9. 小批量真实发送

预览正确后，先发送 5 条：

```powershell
python src\main.py `
  --config config\karlskrona_local.json `
  --send-mode history_data `
  --send-limit 5
```

检查日志：

```text
logs/karlskrona_local_transmission.log
```

确认平台收到的数据正确后，再发送完整数据：

```powershell
python src\main.py `
  --config config\karlskrona_local.json `
  --send-mode history_data `
  --send-limit 0
```

`--send-limit 0` 表示不限制发送数量。

## 10. 历史数据发送

历史数据推荐使用 `auto_runner.py`，它支持状态记录和断点恢复：

```powershell
python src\auto_runner.py `
  --config config\karlskrona_local.json `
  --source-root src\sourceData\history_data\Karlskrona `
  --start-date 2026-08-13 `
  --send-mode history_data `
  --send-limit 0
```

只发送单个文件：

```powershell
python src\auto_runner.py `
  --config config\karlskrona_local.json `
  --source-root src\sourceData\history_data\Karlskrona `
  --file src\sourceData\history_data\Karlskrona\2026-08-13\data.parquet `
  --send-mode history_data
```

状态文件：

```text
state/karlskrona_processed_files.json
```

历史发送中断后，重新执行相同命令即可继续。不要删除或覆盖状态文件。

## 11. 启动实时 watcher

历史数据处理完成后启动长期监听：

```powershell
python src\watch_folder.py
```

默认监听：

```text
src/sourceData/
```

日志：

```text
logs/watch_folder.log
```

watcher 会处理新创建或移动进来的 Parquet 文件，并等待文件大小连续稳定后再发送。已经存在但没有触发新文件事件的文件，应使用 `auto_runner.py` 补发。

## 12. 注册为 Windows 任务计划

使用管理员 PowerShell：

```powershell
$project = "C:\Data_Transmission_Ingrid"
$python = "$project\.venv\Scripts\python.exe"
$action = New-ScheduledTaskAction `
  -Execute $python `
  -Argument "src\watch_folder.py" `
  -WorkingDirectory $project
$trigger = New-ScheduledTaskTrigger -AtStartup
Register-ScheduledTask `
  -TaskName "DataTransmissionWatcher" `
  -Action $action `
  -Trigger $trigger `
  -RunLevel Highest `
  -User "SYSTEM"
```

查看任务：

```powershell
Get-ScheduledTask -TaskName "DataTransmissionWatcher"
```

停止或启动任务：

```powershell
Stop-ScheduledTask -TaskName "DataTransmissionWatcher"
Start-ScheduledTask -TaskName "DataTransmissionWatcher"
```

## 13. 使用 NSSM 注册 Windows 服务（可选）

配置 NSSM：

```text
Application path:
C:\Data_Transmission_Ingrid\.venv\Scripts\python.exe

Startup directory:
C:\Data_Transmission_Ingrid

Arguments:
src\watch_folder.py
```

命令：

```powershell
nssm install DataTransmissionWatcher
nssm start DataTransmissionWatcher
nssm status DataTransmissionWatcher
nssm stop DataTransmissionWatcher
```

## 14. 日常检查

实时查看 watcher 日志：

```powershell
Get-Content logs\watch_folder.log -Wait
```

查看电站日志：

```powershell
Get-Content logs\karlskrona_local_transmission.log -Wait
```

查看发送状态：

```powershell
Get-Content state\karlskrona_processed_files.json
```

重点确认：

- `status` 是否为 `completed`
- 是否出现 `failed`
- `next_offset` 是否持续变化
- `messages_failed` 是否为 `0`

## 15. 服务器重启或升级

先停止 watcher：

```powershell
Stop-ScheduledTask -TaskName "DataTransmissionWatcher"
```

备份状态、配置和日志：

```powershell
Copy-Item state state_backup -Recurse
Copy-Item config config_backup -Recurse
Copy-Item logs logs_backup -Recurse
```

升级代码后重新安装依赖并测试：

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

确认配置、数据和状态文件仍然存在后，再启动 watcher：

```powershell
Start-ScheduledTask -TaskName "DataTransmissionWatcher"
```

## 16. 标准上线顺序

```text
上传项目
  -> 创建 .venv
  -> 安装 requirements.txt
  -> 配置 MQTT 和电站信息
  -> 上传 Parquet
  -> 运行测试
  -> preview 预览
  -> send-limit 发送 5 条
  -> 检查日志和平台结果
  -> auto_runner 发送历史数据
  -> 启动 watch_folder
  -> 注册为 Windows 任务或服务
```

上线时最重要的注意事项：

1. 不要删除或覆盖 `state/*.json`。
2. 不要暴露包含 MQTT 密码的配置文件。
3. 不要只依赖 watcher 处理已经存在的历史文件。
4. 正式全量发送前，必须完成预览和小批量验证。
5. 确认服务器可访问 MQTT host 和对应端口。
