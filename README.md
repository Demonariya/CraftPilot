# CraftPilot

A Windows-first Minecraft server manager built with Rust, Tauri 2, React and TypeScript.

## Current milestone

**1.5.0 — Mod & Plugin Manager Hardening**
- Keep the existing Modrinth-powered plugin/mod discovery, compatibility filtering, inventory, install and removal workflow.
- Verify Modrinth releases against the publisher-provided SHA-512 hash before installation.
- Canonicalize the target add-on folder and reject folder symlinks that resolve outside the server root.
- Stage downloads in unique create-new temporary files and atomically move them into place only after validation.
- Keep add-on changes blocked while the server process is running.

**1.4.0 — Server Automation**
- Per-server auto-start when CraftPilot opens.
- Background process supervision and unexpected-crash detection.
- Configurable restart delay (5–300 seconds) and bounded retry attempts (1–10).
- Restart attempts reset after 60 seconds of stable uptime; manual starts reset the counter.
- Existing server databases are migrated automatically.

**1.3.0 — Startup Profiles & Process Lifecycle**
- Per-server custom JVM arguments with quoted-value parsing and validation.
- Balanced, Performance, and Low Memory launch presets.
- Configurable graceful shutdown timeout (5–120 seconds), followed by forced termination only when necessary.
- Existing databases are migrated automatically without losing server profiles.

**1.2.0 — Server Profile Management**

- Dedicated registered-server management screen
- Edit server display name, Java executable, and minimum/maximum heap allocation
- Validate profile values and prevent edits while the server process is running
- Apply configuration changes on the next server start without moving or modifying server files

**1.1.0 — Java Runtime Lifecycle Management**

- Distinguish CraftPilot-managed Java runtimes from system installations
- Display disk usage for managed runtimes
- Safely remove only CraftPilot-managed runtimes
- Prevent removal while any registered server profile is assigned to the runtime
- Apply the recommended Java major version directly to the selected server

**1.0.0 — Java Download & Compatibility Engine**

- Download official Eclipse Temurin JRE builds for Java 8, 17, 21 and 25 (Windows x64)
- Verify downloaded archives with the publisher-provided SHA-256 checksum
- Safely extract ZIP archives into CraftPilot-managed application data
- Report download and verification progress to the Java Runtime screen
- Discover CraftPilot-managed runtimes alongside system installations
- Recommend Java major versions from Minecraft version input

**0.9.0 — Java Runtime Manager**

- Detect installed Java runtimes from `JAVA_HOME`, `PATH` and common Windows installation directories
- Display runtime vendor, version, major version and executable path
- Assign a separate detected Java executable to each server profile
- Validate Java executables before saving and prevent changing a runtime while its server is running
- Keep existing per-server Java configuration when upgrading
- Java downloads and automatic version recommendations are included in 1.0.0

**0.8.0 — Backup Scheduler & Automation**

- Configure recurring backups from every 15 minutes up to every 7 days
- Keep a configurable number of the newest snapshots (1–100)
- Persist schedules in SQLite and resume them when CraftPilot starts
- Retry in five minutes when a server is running or a backup fails
- Prune old snapshots only after a successful new backup
- Show next run, last successful run and the most recent error
- Scheduler runs locally while CraftPilot is open; it does not install a Windows service or run while the app is closed

**0.7.0 — Backups & Restore**

- Create full local snapshots of a managed server folder, including worlds, configuration and add-ons
- Store snapshots outside the server directory with SQLite metadata
- Restore a snapshot only while the server is stopped
- Automatically create a safety snapshot before every restore
- Stage restored files and roll back if the directory swap fails
- Delete individual snapshots with server-root and backup-root path validation

**0.6.0 — Plugin & Mod Manager**

- Detect Paper, Purpur, Spigot, Fabric, Forge, NeoForge and Quilt from server JAR/folder names
- Detect Minecraft version from the latest server log when available
- Inventory installed plugin/mod JARs from the correct server directory
- Search Modrinth for loader-filtered projects and compatible game versions
- Install the latest compatible Modrinth release with HTTPS/CDN and filename validation
- Remove installed add-ons safely; installation/removal is blocked while the server is running

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

CraftPilot currently manages local Java server processes. Backups are local uncompressed folder snapshots; Windows-service scheduling (backups currently run while CraftPilot is open) and Spigot BuildTools automation remain planned. Java runtime lifecycle controls are available for CraftPilot-managed installations only. Modrinth is the supported add-on source in this release. The Players screen currently provides live roster tracking and console-backed moderation; UUID enrichment is not yet available. The installer currently supports Paper, Purpur and Vanilla.

## License

MIT
