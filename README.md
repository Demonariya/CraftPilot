# CraftPilot

A Windows-first Minecraft server manager built with Rust, Tauri 2, React and TypeScript.

## Current milestone

**0.5.0 — Player Management & Moderation**

- Track online players from standard Minecraft server log events and `/list` output
- Persist player join/leave sessions in SQLite across app restarts
- Send Kick, Ban, Pardon, Whitelist add/remove, OP and De-OP commands from the Players screen
- Refresh the online roster by querying the selected running server
- Validate Minecraft player names and sanitize moderation reasons before sending commands

**0.4.0 — File Manager & Server Properties**

- Browse server directories and files from inside the app
- Read and edit UTF-8 text files up to 1 MB
- Save changes directly to the selected server directory
- Edit `server.properties` with a dedicated editor
- Restrict filesystem operations to the canonical server root; symlink/path traversal escapes are rejected
- Add the Windows application icon required by Tauri bundling

**0.3.0 — Server Installer**

- Install Paper, Purpur and Vanilla server software
- Resolve versions from PaperMC, PurpurMC and Mojang metadata APIs
- Stream server JAR downloads with progress updates and cancellation
- Validate the configured Java executable before installation
- Create a managed server profile and initialize `eula.txt` with `eula=false`
- EULA acceptance remains a manual user action

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

CraftPilot currently manages local Java server processes. Spigot BuildTools automation, backups, plugin/mod management, Java runtime management and scheduling remain planned. The Players screen currently provides live roster tracking and console-backed moderation; UUID enrichment is not yet available. The installer currently supports Paper, Purpur and Vanilla.

## License

MIT
