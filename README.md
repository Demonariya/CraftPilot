# CraftPilot

A Windows-first Minecraft server manager built with Rust, Tauri 2, React and TypeScript.

## Current milestone

**0.2.0 — Server Engine & Live Console**

- Register existing Java Minecraft server folders
- Persist server profiles in SQLite
- Start servers with configurable Java executable and memory limits
- Stream stdout/stderr into the application console
- Send commands through process stdin
- Graceful `stop` followed by forced termination timeout
- Inspect basic host metrics

## Requirements

- Windows 10/11
- Node.js 20.19+ or 22.12+
- Rust stable and the Tauri 2 Windows prerequisites (MSVC Build Tools and WebView2)
- A Java runtime compatible with the selected Minecraft server

## Development

```powershell
npm install
npm run tauri dev
```

## Production build

```powershell
npm run tauri build
```

CraftPilot currently manages local Java server processes. Server software downloading, player management, backups, plugin/mod management and scheduling are planned for later milestones.

## License

MIT
