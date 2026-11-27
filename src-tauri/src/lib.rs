use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::{Path, PathBuf}, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, time::Duration};
use sysinfo::System;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, process::{Child, ChildStdin, Command}, time::sleep};
use uuid::Uuid;
use futures_util::StreamExt;
use serde_json::Value;
use sha2::{Digest, Sha256};

struct AppState { db: Mutex<Connection>, processes: tokio::sync::Mutex<HashMap<String, RunningServer>>, backup_root: PathBuf }
struct RunningServer { child: Child, stdin: ChildStdin }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerProfile { id:String, name:String, directory:String, jar_file:String, java_path:String, min_memory_mb:u32, max_memory_mb:u32, created_at:String, status:String }
#[derive(Serialize, Clone)]
struct ServerOutput { server_id:String, stream:String, line:String, timestamp:String }
#[derive(Serialize)]
struct HostMetrics { cpu_usage:f32, used_memory_mb:u64, total_memory_mb:u64 }
fn emit_line(app:&AppHandle, server_id:&str, stream:&str, line:String) { let payload=ServerOutput{server_id:server_id.into(),stream:stream.into(),line,timestamp:Utc::now().to_rfc3339()};let _=app.emit("server://output",payload); }
fn db_conn<'a>(state:&'a State<'a,AppState>)->Result<std::sync::MutexGuard<'a,Connection>,String>{state.db.lock().map_err(|e|format!("Database lock failed: {e}"))}

#[derive(Serialize, Clone)]
struct ServerPlayer { name:String, uuid:Option<String>, joined_at:String }
#[derive(Serialize, Clone)]
struct PlayerSession { id:i64, player_name:String, joined_at:String, left_at:Option<String> }
fn record_player_line(app:&AppHandle,server_id:&str,line:&str) {
 let state=app.state::<AppState>();
 if let Some(pos)=line.find("players online:") {
  let names=line[pos+"players online:".len()..].trim();
  if let Ok(conn)=state.db.lock() {
   let _=conn.execute("DELETE FROM online_players WHERE server_id=?1",[server_id]);
   for name in names.split(',').map(str::trim).filter(|n|!n.is_empty()) {
    let _=conn.execute("INSERT OR REPLACE INTO online_players(server_id,player_name,joined_at) VALUES(?1,?2,COALESCE((SELECT joined_at FROM online_players WHERE server_id=?1 AND player_name=?2),?3))",params![server_id,name,Utc::now().to_rfc3339()]);
   }
  }
 }
 let Some(message)=line.split_once(": ").map(|(_,m)|m.trim()) else{return};
 let (name,joined)=if let Some(n)=message.strip_suffix(" joined the game"){(n.trim(),true)}else if let Some(n)=message.strip_suffix(" left the game"){(n.trim(),false)}else{return};
 if name.is_empty()||name.len()>16||!name.chars().all(|c|c.is_ascii_alphanumeric()||c=='_'){return;}
 if let Ok(conn)=state.db.lock(){let now=Utc::now().to_rfc3339();if joined{let _=conn.execute("INSERT OR REPLACE INTO online_players(server_id,player_name,joined_at) VALUES(?1,?2,?3)",[server_id,name,&now]);let _=conn.execute("INSERT INTO player_sessions(server_id,player_name,joined_at) VALUES(?1,?2,?3)",[server_id,name,&now]);}else{let _=conn.execute("DELETE FROM online_players WHERE server_id=?1 AND player_name=?2",params![server_id,name]);let _=conn.execute("UPDATE player_sessions SET left_at=?1 WHERE id=(SELECT id FROM player_sessions WHERE server_id=?2 AND player_name=?3 AND left_at IS NULL ORDER BY id DESC LIMIT 1)",params![now,server_id,name]);}};
}
fn valid_player_name(name:&str)->bool{!name.is_empty()&&name.len()<=16&&name.chars().all(|c|c.is_ascii_alphanumeric()||c=='_')}
#[tauri::command]
fn get_server_players(server_id:String,state:State<'_,AppState>)->Result<Vec<ServerPlayer>,String>{let conn=db_conn(&state)?;let mut stmt=conn.prepare("SELECT player_name,NULL,joined_at FROM online_players WHERE server_id=?1 ORDER BY player_name COLLATE NOCASE").map_err(|e|e.to_string())?;let rows=stmt.query_map([server_id],|r|Ok(ServerPlayer{name:r.get(0)?,uuid:r.get(1)?,joined_at:r.get(2)?})).map_err(|e|e.to_string())?;rows.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())}
#[tauri::command]
fn get_player_history(server_id:String,limit:Option<u32>,state:State<'_,AppState>)->Result<Vec<PlayerSession>,String>{let conn=db_conn(&state)?;let mut stmt=conn.prepare("SELECT id,player_name,joined_at,left_at FROM player_sessions WHERE server_id=?1 ORDER BY id DESC LIMIT ?2").map_err(|e|e.to_string())?;let rows=stmt.query_map(params![server_id,limit.unwrap_or(50).min(200)],|r|Ok(PlayerSession{id:r.get(0)?,player_name:r.get(1)?,joined_at:r.get(2)?,left_at:r.get(3)?})).map_err(|e|e.to_string())?;rows.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())}
#[tauri::command]
async fn refresh_server_players(server_id:String,state:State<'_,AppState>)->Result<(),String>{send_command_inner(&server_id,"list",&state).await}
async fn send_command_inner(server_id:&str,command:&str,state:&State<'_,AppState>)->Result<(),String>{let mut processes=state.processes.lock().await;let finished=match processes.get_mut(server_id){Some(entry)=>entry.child.try_wait().map_err(|e|e.to_string())?.is_some(),None=>return Err("Server process is not running".into())};if finished{processes.remove(server_id);return Err("Server process has already exited".into());}let entry=processes.get_mut(server_id).ok_or("Server process is not running")?;entry.stdin.write_all(command.as_bytes()).await.map_err(|e|e.to_string())?;entry.stdin.write_all(b"\n").await.map_err(|e|e.to_string())?;entry.stdin.flush().await.map_err(|e|e.to_string())}
#[tauri::command]
async fn manage_player(server_id:String,action:String,player_name:String,reason:Option<String>,state:State<'_,AppState>)->Result<(),String>{if !valid_player_name(&player_name){return Err("Player name must contain 1–16 letters, numbers or underscores".into());}let reason=reason.unwrap_or_default().chars().filter(|c|!c.is_control()).take(120).collect::<String>();let command=match action.as_str(){"kick"=>format!("kick {player_name} {}",if reason.is_empty(){"".into()}else{format!("{reason}")}),"ban"=>format!("ban {player_name} {}",if reason.is_empty(){"".into()}else{reason}),"pardon"=>format!("pardon {player_name}"),"whitelist_add"=>format!("whitelist add {player_name}"),"whitelist_remove"=>format!("whitelist remove {player_name}"),"op"=>format!("op {player_name}"),"deop"=>format!("deop {player_name}"),_=>return Err("Unsupported player action".into())};send_command_inner(&server_id,&command,&state).await}

#[tauri::command]
async fn list_servers(state:State<'_,AppState>)->Result<Vec<ServerProfile>,String>{
 let mut servers={
  let conn=db_conn(&state)?;
  let mut stmt=conn.prepare("SELECT id,name,directory,jar_file,java_path,min_memory_mb,max_memory_mb,created_at FROM servers ORDER BY created_at DESC").map_err(|e|e.to_string())?;
  let rows=stmt.query_map([],|row|Ok(ServerProfile{id:row.get(0)?,name:row.get(1)?,directory:row.get(2)?,jar_file:row.get(3)?,java_path:row.get(4)?,min_memory_mb:row.get(5)?,max_memory_mb:row.get(6)?,created_at:row.get(7)?,status:"stopped".into()})).map_err(|e|e.to_string())?;
  rows.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?
 };
 let mut processes=state.processes.lock().await;let mut finished=Vec::new();
 for server in &mut servers { if let Some(entry)=processes.get_mut(&server.id) { match entry.child.try_wait() { Ok(Some(_))=>finished.push(server.id.clone()),Ok(None)=>server.status="running".into(),Err(e)=>return Err(format!("Failed to inspect server process: {e}")) } } }
 for id in finished { processes.remove(&id); }
 Ok(servers)
}

#[tauri::command]
fn register_server(name:String,directory:String,jar_file:String,java_path:String,min_memory_mb:u32,max_memory_mb:u32,state:State<'_,AppState>)->Result<ServerProfile,String>{
 let root=PathBuf::from(&directory);if !root.is_dir(){return Err("The selected server directory does not exist.".into());}
 if jar_file.trim().is_empty()||!root.join(&jar_file).is_file(){return Err("The selected JAR file was not found inside the server directory.".into());}
 if name.trim().is_empty(){return Err("Server name cannot be empty.".into());}
 if min_memory_mb==0||max_memory_mb<min_memory_mb{return Err("Memory limits are invalid: maximum must be at least the minimum.".into());}
 let profile=ServerProfile{id:Uuid::new_v4().to_string(),name:name.trim().to_string(),directory,jar_file,java_path,min_memory_mb,max_memory_mb,created_at:Utc::now().to_rfc3339(),status:"stopped".into()};
 let conn=db_conn(&state)?;conn.execute("INSERT INTO servers (id,name,directory,jar_file,java_path,min_memory_mb,max_memory_mb,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",params![profile.id,profile.name,profile.directory,profile.jar_file,profile.java_path,profile.min_memory_mb,profile.max_memory_mb,profile.created_at]).map_err(|e|e.to_string())?;Ok(profile)
}

#[tauri::command]
async fn start_server(server_id:String,app:AppHandle,state:State<'_,AppState>)->Result<(),String>{
 let profile={let conn=db_conn(&state)?;conn.query_row("SELECT id,name,directory,jar_file,java_path,min_memory_mb,max_memory_mb,created_at FROM servers WHERE id=?1",[&server_id],|row|Ok(ServerProfile{id:row.get(0)?,name:row.get(1)?,directory:row.get(2)?,jar_file:row.get(3)?,java_path:row.get(4)?,min_memory_mb:row.get(5)?,max_memory_mb:row.get(6)?,created_at:row.get(7)?,status:"stopped".into()})).map_err(|e|format!("Server profile not found: {e}"))?};
 let mut processes=state.processes.lock().await;
 if let Some(entry)=processes.get_mut(&server_id){if entry.child.try_wait().map_err(|e|e.to_string())?.is_none(){return Err("This server is already running.".into());}}
 processes.remove(&server_id);
 let mut child=Command::new(&profile.java_path).arg(format!("-Xms{}M",profile.min_memory_mb)).arg(format!("-Xmx{}M",profile.max_memory_mb)).arg("-jar").arg(&profile.jar_file).arg("nogui").current_dir(&profile.directory).stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).kill_on_drop(true).spawn().map_err(|e|format!("Failed to launch Java process: {e}"))?;
 let stdin=child.stdin.take().ok_or("Could not capture server stdin")?;let stdout=child.stdout.take().ok_or("Could not capture server stdout")?;let stderr=child.stderr.take().ok_or("Could not capture server stderr")?;
 let out_app=app.clone();let out_id=server_id.clone();tauri::async_runtime::spawn(async move{let mut lines=BufReader::new(stdout).lines();while let Ok(Some(line))=lines.next_line().await{record_player_line(&out_app,&out_id,&line);emit_line(&out_app,&out_id,"stdout",line);}});
 let err_app=app.clone();let err_id=server_id.clone();tauri::async_runtime::spawn(async move{let mut lines=BufReader::new(stderr).lines();while let Ok(Some(line))=lines.next_line().await{record_player_line(&err_app,&err_id,&line);emit_line(&err_app,&err_id,"stderr",line);}});
 emit_line(&app,&server_id,"system",format!("Starting {} with {}",profile.name,profile.java_path));processes.insert(server_id,RunningServer{child,stdin});Ok(())
}

#[tauri::command]
async fn send_server_command(server_id:String,command:String,state:State<'_,AppState>)->Result<(),String>{
 let mut processes=state.processes.lock().await;
 let finished=match processes.get_mut(&server_id){Some(entry)=>entry.child.try_wait().map_err(|e|e.to_string())?.is_some(),None=>return Err("Server process is not running".into())};
 if finished{processes.remove(&server_id);return Err("Server process has already exited.".into());}
 let entry=processes.get_mut(&server_id).ok_or("Server process is not running")?;
 entry.stdin.write_all(command.trim().as_bytes()).await.map_err(|e|e.to_string())?;entry.stdin.write_all(b"\n").await.map_err(|e|e.to_string())?;entry.stdin.flush().await.map_err(|e|e.to_string())
}

#[tauri::command]
async fn stop_server(server_id:String,app:AppHandle,state:State<'_,AppState>)->Result<(),String>{
 let mut processes=state.processes.lock().await;let mut entry=processes.remove(&server_id).ok_or("Server process is not running")?;
 if entry.child.try_wait().map_err(|e|e.to_string())?.is_some(){emit_line(&app,&server_id,"system","Server process had already exited.".into());return Ok(());}
 let _=entry.stdin.write_all(b"stop\n").await;let _=entry.stdin.flush().await;
 for _ in 0..100{sleep(Duration::from_millis(100)).await;if entry.child.try_wait().map_err(|e|e.to_string())?.is_some(){emit_line(&app,&server_id,"system","Server stopped gracefully.".into());return Ok(());}}
 entry.child.kill().await.map_err(|e|format!("Failed to terminate server: {e}"))?;emit_line(&app,&server_id,"system","Graceful shutdown timed out; process was terminated.".into());Ok(())
}

#[derive(Serialize, Clone)]
struct JavaRuntime { path:String, version:String, major:u32, vendor:String, managed:bool, size_bytes:u64 }
fn directory_size(path:&Path)->u64{let Ok(entries)=fs::read_dir(path) else{return 0};entries.flatten().map(|e|{let Ok(t)=e.file_type() else{return 0};if t.is_symlink(){0}else if t.is_dir(){directory_size(&e.path())}else{e.metadata().map(|m|m.len()).unwrap_or(0)}}).sum()}
fn inspect_java(path:&PathBuf)->Option<JavaRuntime>{
 if !path.is_file() && path.to_string_lossy()!="java" { return None; }
 let output=std::process::Command::new(path).arg("-version").output().ok()?;
 let text=format!("{}\n{}",String::from_utf8_lossy(&output.stdout),String::from_utf8_lossy(&output.stderr));
 let version=text.lines().find(|l|l.contains("version")).unwrap_or(text.lines().next()? ).trim().to_string();
 let quoted=version.split('"').nth(1).unwrap_or("");
 let first=quoted.split(['.', '_', '+', '-']).next().unwrap_or("");
 let mut major=first.parse::<u32>().ok()?;
 if major==1 { major=quoted.split('.').nth(1)?.parse().ok()?; }
 if major==0 { return None; }
 let lower=text.to_lowercase();
 let vendor=if lower.contains("temurin")||lower.contains("adoptium"){"Eclipse Temurin"}else if lower.contains("zulu"){"Azul Zulu"}else if lower.contains("microsoft"){"Microsoft Build of OpenJDK"}else if lower.contains("graalvm"){"GraalVM"}else if lower.contains("oracle"){"Oracle JDK"}else if lower.contains("openjdk"){"OpenJDK"}else{"Java Runtime"}.to_string();
 Some(JavaRuntime{path:path.to_string_lossy().to_string(),version,major,vendor,managed:false,size_bytes:0})
}
#[tauri::command]
fn list_java_runtimes(app:AppHandle)->Vec<JavaRuntime>{
 let mut candidates=Vec::<PathBuf>::new();
 let exe=if cfg!(windows){"java.exe"}else{"java"};
 if let Some(home)=std::env::var_os("JAVA_HOME"){candidates.push(PathBuf::from(home).join("bin").join(exe));}
 if let Some(path)=std::env::var_os("PATH"){for dir in std::env::split_paths(&path){candidates.push(dir.join(exe));}}
 #[cfg(windows)] {
  for key in ["ProgramFiles","ProgramFiles(x86)"] { if let Some(root)=std::env::var_os(key){let root=PathBuf::from(root);for vendor in ["Java","Eclipse Adoptium","Microsoft","Zulu","Amazon Corretto","BellSoft"] {let dir=root.join(vendor);if let Ok(entries)=fs::read_dir(dir){for entry in entries.flatten(){candidates.push(entry.path().join("bin").join(exe));}}}} }
 }
 if let Ok(data)=app.path().app_data_dir(){let managed=data.join("java");if let Ok(entries)=fs::read_dir(managed){for entry in entries.flatten(){candidates.push(entry.path().join("bin").join(exe));}}}
 let managed_root=app.path().app_data_dir().ok().and_then(|p|fs::canonicalize(p.join("java")).ok());
 let mut seen=std::collections::HashSet::new();let mut runtimes=Vec::new();
 for path in candidates {let key=fs::canonicalize(&path).unwrap_or_else(|_|path.clone()).to_string_lossy().to_lowercase();if !seen.insert(key){continue;}if let Some(mut runtime)=inspect_java(&path){if let Some(root)=&managed_root{if let Ok(exe)=fs::canonicalize(&path){if let Some(home)=exe.parent().and_then(Path::parent){runtime.managed=home.parent()==Some(root.as_path());if runtime.managed{runtime.size_bytes=directory_size(home);}}}}runtimes.push(runtime);}}
 runtimes.sort_by(|a,b|b.major.cmp(&a.major).then_with(||a.vendor.cmp(&b.vendor)));runtimes
}
#[tauri::command]
async fn set_server_java(server_id:String,java_path:String,state:State<'_,AppState>)->Result<(),String>{
 let path=PathBuf::from(&java_path);let runtime=inspect_java(&path).ok_or("Selected Java executable is invalid or could not be started.")?;
 let processes=state.processes.lock().await;if processes.contains_key(&server_id){return Err("Stop this server before changing its Java runtime.".into());}drop(processes);
 let conn=db_conn(&state)?;let changed=conn.execute("UPDATE servers SET java_path=?1 WHERE id=?2",params![runtime.path,server_id]).map_err(|e|e.to_string())?;if changed==0{return Err("Server profile not found.".into());}Ok(())
}
#[tauri::command]
fn remove_managed_java(java_path:String,app:AppHandle,state:State<'_,AppState>)->Result<(),String>{
 let root=app.path().app_data_dir().map_err(|e|e.to_string())?.join("java");let root=fs::canonicalize(&root).map_err(|_|"No managed Java runtimes directory exists.")?;
 let executable=fs::canonicalize(PathBuf::from(&java_path)).map_err(|_|"Java executable no longer exists.")?;
 let home=executable.parent().and_then(Path::parent).ok_or("Cannot resolve Java home directory.")?;
 if home.parent()!=Some(root.as_path()){return Err("Only CraftPilot-managed Java runtimes can be removed here.".into());}
 let conn=db_conn(&state)?;let mut stmt=conn.prepare("SELECT name,java_path FROM servers").map_err(|e|e.to_string())?;
 let rows=stmt.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).map_err(|e|e.to_string())?;
 for row in rows{let (name,path)=row.map_err(|e|e.to_string())?;if fs::canonicalize(PathBuf::from(path)).map(|p|p==executable).unwrap_or(false){return Err(format!("Cannot remove this runtime because server '{name}' is assigned to it. Reassign that server first."));}}
 drop(stmt);drop(conn);fs::remove_dir_all(home).map_err(|e|format!("Failed to remove managed Java runtime: {e}"))?;Ok(())
}
#[tauri::command]
async fn install_java_runtime(major:u32,app:AppHandle)->Result<JavaRuntime,String>{
 if ![8,17,21,25].contains(&major){return Err("Supported Java versions are 8, 17, 21 and 25.".into());}
 let data=app.path().app_data_dir().map_err(|e|e.to_string())?;let root=data.join("java");fs::create_dir_all(&root).map_err(|e|e.to_string())?;
 let target=root.join(format!("temurin-{major}"));let exe=if cfg!(windows){"java.exe"}else{"java"};let existing=target.join("bin").join(exe);if let Some(runtime)=inspect_java(&existing){return Ok(runtime);}
 let id=Uuid::new_v4().to_string();let api=format!("https://api.adoptium.net/v3/assets/latest/{major}/hotspot?architecture=x64&heap_size=normal&image_type=jre&jvm_impl=hotspot&os=windows&page_size=1&vendor=eclipse");
 emit_install(&app,&id,0,0,"metadata",&format!("Resolving Eclipse Temurin Java {major} package…"));
 let metadata=fetch_json(&api).await?;let asset=metadata.as_array().and_then(|a|a.first()).ok_or("Eclipse Adoptium has no matching Windows x64 JRE release for this Java version.")?;
 let package=asset.get("binary").and_then(|b|b.get("package")).ok_or("Java package metadata is incomplete.")?;
 let url=package.get("link").and_then(Value::as_str).ok_or("Java download URL is missing.")?;
 let expected=package.get("checksum").and_then(Value::as_str).ok_or("Java package checksum is missing.")?.to_lowercase();
 let client=reqwest::Client::new();let response=client.get(url).header("User-Agent","CraftPilot/1.0.0").send().await.map_err(|e|format!("Java download failed: {e}"))?;
 if !response.status().is_success(){return Err(format!("Java download server returned HTTP {}",response.status()));}
 let total=response.content_length().unwrap_or(0);let temp=root.join(format!(".java-{id}.zip"));let mut file=tokio::fs::File::create(&temp).await.map_err(|e|e.to_string())?;let mut stream=response.bytes_stream();let mut downloaded=0u64;
 while let Some(chunk)=stream.next().await{let chunk=chunk.map_err(|e|format!("Java download stream failed: {e}"))?;file.write_all(&chunk).await.map_err(|e|e.to_string())?;downloaded+=chunk.len() as u64;emit_install(&app,&id,downloaded,total,"download",&format!("Downloading Java {major} · {:.1} MB",downloaded as f64/1048576.0));}
 file.flush().await.map_err(|e|e.to_string())?;drop(file);
 let bytes=fs::read(&temp).map_err(|e|e.to_string())?;let actual=format!("{:x}",Sha256::digest(&bytes));if actual!=expected{let _=fs::remove_file(&temp);return Err("Java archive SHA-256 verification failed. The archive was discarded.".into());}
 emit_install(&app,&id,downloaded,total,"verify","Checksum verified. Extracting Java runtime…");
 let stage=root.join(format!(".extract-{id}"));fs::create_dir_all(&stage).map_err(|e|e.to_string())?;let archive_path=temp.clone();let stage_path=stage.clone();
 let extracted=tokio::task::spawn_blocking(move||->Result<(),String>{
  let file=std::fs::File::open(&archive_path).map_err(|e|e.to_string())?;let mut archive=zip::ZipArchive::new(file).map_err(|e|format!("Invalid Java ZIP archive: {e}"))?;
  for index in 0..archive.len(){let mut entry=archive.by_index(index).map_err(|e|e.to_string())?;let relative=entry.enclosed_name().ok_or("Java archive contains an unsafe path.")?.to_path_buf();let out=stage_path.join(relative);if !out.starts_with(&stage_path){return Err("Java archive attempted to escape its extraction directory.".into());}
   if entry.is_dir(){std::fs::create_dir_all(&out).map_err(|e|e.to_string())?;}else{if entry.unix_mode().map(|m|m&0o170000==0o120000).unwrap_or(false){return Err("Java archive contains an unexpected symbolic link.".into());}if let Some(parent)=out.parent(){std::fs::create_dir_all(parent).map_err(|e|e.to_string())?;}let mut output=std::fs::File::create(&out).map_err(|e|e.to_string())?;std::io::copy(&mut entry,&mut output).map_err(|e|e.to_string())?;}
  }Ok(())
 }).await.map_err(|e|format!("Java extraction task failed: {e}"))?;
 let _=fs::remove_file(&temp);if let Err(error)=extracted{let _=fs::remove_dir_all(&stage);return Err(error);}
 let mut runtime_exe=None;for entry in fs::read_dir(&stage).map_err(|e|e.to_string())?.flatten(){let candidate=entry.path().join("bin").join(exe);if candidate.is_file(){runtime_exe=Some(candidate);break;}}
 let source=runtime_exe.ok_or("The downloaded archive does not contain a valid Java executable.")?;let source_root=source.parent().and_then(Path::parent).ok_or("Cannot resolve extracted Java home directory.")?.to_path_buf();
 if target.exists(){fs::remove_dir_all(&target).map_err(|e|format!("Cannot replace previous managed Java runtime: {e}"))?;}
 fs::rename(&source_root,&target).map_err(|e|format!("Cannot finalize Java installation: {e}"))?;let _=fs::remove_dir_all(&stage);
 let runtime=inspect_java(&target.join("bin").join(exe)).ok_or("Java was extracted but failed its version check.")?;
 emit_install(&app,&id,downloaded,total,"complete",&format!("Java {major} installed successfully."));Ok(runtime)
}
#[tauri::command]
fn get_java_recommendation(minecraft_version:String)->Result<(u32,String),String>{
 let version=minecraft_version.trim().trim_start_matches('v');let mut parts=version.split('.');let major=parts.next().and_then(|v|v.parse::<u32>().ok()).ok_or("Enter a valid Minecraft version, such as 1.21.8 or 26.1.")?;let minor=parts.next().and_then(|v|v.parse::<u32>().ok()).unwrap_or(0);
 let java=if major>=26{25}else if major>1||(major==1&&minor>=20&&version.split('.').nth(2).and_then(|v|v.parse::<u32>().ok()).unwrap_or(0)>=5){21}else if major==1&&minor>=17{17}else{8};
 let reason=match java{25=>"Minecraft 26.1 and newer require Java 25.",21=>"Minecraft 1.20.5 through 1.21.x use Java 21.",17=>"Minecraft 1.17 through 1.20.4 use Java 17.",_=>"Legacy Minecraft versions before 1.17 generally use Java 8."}.to_string();Ok((java,reason))
}
#[tauri::command]
fn get_host_metrics()->HostMetrics{let mut system=System::new_all();system.refresh_all();HostMetrics{cpu_usage:system.global_cpu_usage(),used_memory_mb:system.used_memory()/1024/1024,total_memory_mb:system.total_memory()/1024/1024}}


#[derive(Default)]
struct InstallerState { cancellations: Mutex<HashMap<String, Arc<AtomicBool>>> }
#[derive(Serialize, Clone)]
struct InstallProgress { install_id:String, downloaded:u64, total:u64, percent:f32, stage:String, message:String }
fn emit_install(app:&AppHandle,id:&str,downloaded:u64,total:u64,stage:&str,message:&str){let p=InstallProgress{install_id:id.into(),downloaded,total,percent:if total>0{downloaded as f32/total as f32*100.0}else{0.0},stage:stage.into(),message:message.into()};let _=app.emit("installer://progress",p);}
async fn fetch_json(url:&str)->Result<Value,String>{let response=reqwest::Client::new().get(url).header("User-Agent","CraftPilot/0.4.0").send().await.map_err(|e|format!("Metadata request failed: {e}"))?;if !response.status().is_success(){return Err(format!("Metadata service returned HTTP {}",response.status()));}response.json::<Value>().await.map_err(|e|format!("Invalid metadata response: {e}"))}
#[tauri::command]
async fn list_installable_versions(engine:String)->Result<Vec<String>,String>{let versions=match engine.as_str(){"paper"=>fetch_json("https://api.papermc.io/v2/projects/paper").await?.get("versions").and_then(Value::as_array).ok_or("Paper version list is unavailable")?.iter().filter_map(Value::as_str).map(str::to_owned).collect(),"purpur"=>fetch_json("https://api.purpurmc.org/v2/purpur").await?.get("versions").and_then(Value::as_array).ok_or("Purpur version list is unavailable")?.iter().filter_map(Value::as_str).map(str::to_owned).collect(),"vanilla"=>fetch_json("https://piston-meta.mojang.com/mc/game/version_manifest_v2.json").await?.get("versions").and_then(Value::as_array).ok_or("Mojang version manifest is unavailable")?.iter().filter_map(|v|v.get("id").and_then(Value::as_str).map(str::to_owned)).collect(),_=>return Err("Unsupported server software. Choose Paper, Purpur or Vanilla.".into())};Ok(versions)}
async fn resolve_server_download(engine:&str,version:&str)->Result<(String,String),String>{match engine{"paper"=>{let v=fetch_json(&format!("https://api.papermc.io/v2/projects/paper/versions/{version}")).await?;let builds=v.get("builds").and_then(Value::as_array).ok_or("No Paper builds found for this Minecraft version")?;let build=builds.last().and_then(Value::as_u64).ok_or("No Paper build is available")?;let meta=fetch_json(&format!("https://api.papermc.io/v2/projects/paper/versions/{version}/builds/{build}")).await?;let name=meta.get("downloads").and_then(|d|d.get("application")).and_then(|a|a.get("name")).and_then(Value::as_str).unwrap_or("paper-server.jar").to_owned();Ok((format!("https://api.papermc.io/v2/projects/paper/versions/{version}/builds/{build}/downloads/{name}"),name))},"purpur"=>{let meta=fetch_json(&format!("https://api.purpurmc.org/v2/purpur/{version}")).await?;let build=meta.get("builds").and_then(|b|b.get("latest")).and_then(Value::as_str).ok_or("No Purpur build is available")?;Ok((format!("https://api.purpurmc.org/v2/purpur/{version}/{build}/download"),format!("purpur-{version}.jar")))},"vanilla"=>{let manifest=fetch_json("https://piston-meta.mojang.com/mc/game/version_manifest_v2.json").await?;let entry=manifest.get("versions").and_then(Value::as_array).and_then(|a|a.iter().find(|v|v.get("id").and_then(Value::as_str)==Some(version))).ok_or("Minecraft version was not found in Mojang's manifest")?;let meta_url=entry.get("url").and_then(Value::as_str).ok_or("Version metadata URL is missing")?;let meta=fetch_json(meta_url).await?;let url=meta.get("downloads").and_then(|d|d.get("server")).and_then(|s|s.get("url")).and_then(Value::as_str).ok_or("This Minecraft version does not publish a vanilla server JAR")?;Ok((url.to_owned(),format!("minecraft_server.{version}.jar")))},_=>Err("Unsupported server software".into())}}
#[tauri::command]
async fn cancel_server_install(install_id:String,installer:State<'_,InstallerState>)->Result<(),String>{let map=installer.cancellations.lock().map_err(|e|e.to_string())?;let token=map.get(&install_id).ok_or("Install task was not found")?;token.store(true,Ordering::SeqCst);Ok(())}
#[tauri::command]
async fn install_server(engine:String,version:String,name:String,directory:String,java_path:String,min_memory_mb:u32,max_memory_mb:u32,app:AppHandle,state:State<'_,AppState>,installer:State<'_,InstallerState>)->Result<ServerProfile,String>{
 if name.trim().is_empty(){return Err("Server name cannot be empty".into());}if min_memory_mb==0||max_memory_mb<min_memory_mb{return Err("Invalid memory allocation".into());}if !["paper","purpur","vanilla"].contains(&engine.as_str()){return Err("Unsupported server software".into());}
 let root=PathBuf::from(&directory);fs::create_dir_all(&root).map_err(|e|format!("Cannot create server directory: {e}"))?;let java=Command::new(&java_path).arg("-version").output().await.map_err(|e|format!("Java runtime could not be started: {e}"))?;if !java.status.success(){return Err("The selected Java runtime did not pass its version check".into());}
 let id=Uuid::new_v4().to_string();let cancelled=Arc::new(AtomicBool::new(false));installer.cancellations.lock().map_err(|e|e.to_string())?.insert(id.clone(),cancelled.clone());
 let result:Result<ServerProfile,String>=async{
  emit_install(&app,&id,0,0,"metadata","Resolving server download…");let (url,jar_name)=resolve_server_download(&engine,&version).await?;let response=reqwest::Client::new().get(&url).header("User-Agent","CraftPilot/0.4.0").send().await.map_err(|e|format!("Server download failed: {e}"))?;if !response.status().is_success(){return Err(format!("Download server returned HTTP {}",response.status()));}let total=response.content_length().unwrap_or(0);let jar_path=root.join(&jar_name);let part_path=root.join(format!("{jar_name}.part"));let mut file=tokio::fs::File::create(&part_path).await.map_err(|e|e.to_string())?;let mut stream=response.bytes_stream();let mut downloaded=0u64;
  while let Some(chunk)=stream.next().await{if cancelled.load(Ordering::SeqCst){drop(file);let _=tokio::fs::remove_file(&part_path).await;return Err("Installation cancelled".into());}let chunk=chunk.map_err(|e|format!("Download stream failed: {e}"))?;file.write_all(&chunk).await.map_err(|e|e.to_string())?;downloaded+=chunk.len() as u64;emit_install(&app,&id,downloaded,total,"downloading","Downloading server JAR…");}
  file.flush().await.map_err(|e|e.to_string())?;drop(file);if downloaded==0{return Err("Downloaded file is empty".into());}tokio::fs::rename(&part_path,&jar_path).await.map_err(|e|format!("Cannot finalize server JAR: {e}"))?;
  tokio::fs::write(root.join("eula.txt"),"# By changing the setting below to TRUE you are indicating your agreement to the Minecraft EULA (https://aka.ms/MinecraftEULA).\neula=false\n").await.map_err(|e|e.to_string())?;
  let profile=ServerProfile{id:Uuid::new_v4().to_string(),name:name.trim().into(),directory:directory.clone(),jar_file:jar_name,java_path:java_path.clone(),min_memory_mb,max_memory_mb,created_at:Utc::now().to_rfc3339(),status:"stopped".into()};let conn=db_conn(&state)?;conn.execute("INSERT INTO servers (id,name,directory,jar_file,java_path,min_memory_mb,max_memory_mb,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",params![profile.id,profile.name,profile.directory,profile.jar_file,profile.java_path,profile.min_memory_mb,profile.max_memory_mb,profile.created_at]).map_err(|e|e.to_string())?;emit_install(&app,&id,downloaded,total,"completed","Server installed. Review and accept the EULA before starting it.");Ok(profile)
 }.await;
 if result.is_err(){emit_install(&app,&id,0,0,"failed","Installation failed or was cancelled.");}if let Ok(mut map)=installer.cancellations.lock(){map.remove(&id);}result
}


#[derive(Serialize, Clone)]
struct ServerRuntimeInfo { software:String, loader:String, minecraft_version:Option<String>, addon_kind:String, supported:bool }
#[derive(Serialize, Clone)]
struct InstalledAddon { name:String, file_name:String, size:u64, kind:String }
#[derive(Serialize, Clone)]
struct ModrinthProject { project_id:String, title:String, description:String, slug:String, downloads:u64, icon_url:Option<String>, categories:Vec<String> }
fn server_profile_for_addons(state:&State<'_,AppState>,server_id:&str)->Result<(String,String),String>{let conn=db_conn(state)?;conn.query_row("SELECT directory,jar_file FROM servers WHERE id=?1",[server_id],|r|Ok((r.get(0)?,r.get(1)?))).map_err(|e|format!("Server profile not found: {e}"))}
fn detect_runtime(directory:&str,jar_file:&str)->ServerRuntimeInfo{
 let text=format!("{} {}",jar_file.to_lowercase(),directory.to_lowercase());
 let (software,loader,addon_kind,supported)=if text.contains("paper"){("Paper","paper","plugin",true)}else if text.contains("purpur"){("Purpur","purpur","plugin",true)}else if text.contains("spigot")||text.contains("craftbukkit"){("Spigot","spigot","plugin",true)}else if text.contains("neoforge")||text.contains("neo-forge"){("NeoForge","neoforge","mod",true)}else if text.contains("forge"){("Forge","forge","mod",true)}else if text.contains("fabric"){("Fabric","fabric","mod",true)}else if text.contains("quilt"){("Quilt","quilt","mod",true)}else{("Vanilla","vanilla","none",false)};
 let log=PathBuf::from(directory).join("logs").join("latest.log");let log_text=fs::read_to_string(log).unwrap_or_default();let mut minecraft_version=None;
 for line in log_text.lines().rev().take(4000){if let Some(i)=line.find("Starting minecraft server version "){let v=line[i+"Starting minecraft server version ".len()..].split_whitespace().next().unwrap_or("").trim_matches(|c:char|!c.is_ascii_alphanumeric()&&c!='.' );if !v.is_empty(){minecraft_version=Some(v.to_owned());break;}}if let Some(i)=line.find("Loading Minecraft "){let v=line[i+"Loading Minecraft ".len()..].split_whitespace().next().unwrap_or("").trim_matches(|c:char|!c.is_ascii_alphanumeric()&&c!='.');if !v.is_empty(){minecraft_version=Some(v.to_owned());break;}}}
 ServerRuntimeInfo{software:software.into(),loader:loader.into(),minecraft_version,addon_kind:addon_kind.into(),supported}
}
#[tauri::command]
fn detect_server_runtime(server_id:String,state:State<'_,AppState>)->Result<ServerRuntimeInfo,String>{let(directory,jar)=server_profile_for_addons(&state,&server_id)?;Ok(detect_runtime(&directory,&jar))}
#[tauri::command]
fn list_server_addons(server_id:String,kind:String,state:State<'_,AppState>)->Result<Vec<InstalledAddon>,String>{if !["plugin","mod"].contains(&kind.as_str()){return Err("Addon type must be plugin or mod".into());}let(directory,jar)=server_profile_for_addons(&state,&server_id)?;let runtime=detect_runtime(&directory,&jar);if (kind=="plugin"&&runtime.addon_kind!="plugin")||(kind=="mod"&&runtime.addon_kind!="mod"){return Err(format!("{} servers do not use this addon type",runtime.software));}let folder=PathBuf::from(directory).join(if kind=="plugin"{"plugins"}else{"mods"});if !folder.exists(){return Ok(Vec::new());}let mut items=Vec::new();for entry in fs::read_dir(&folder).map_err(|e|e.to_string())?{let entry=entry.map_err(|e|e.to_string())?;let path=entry.path();if path.is_file()&&path.extension().and_then(|x|x.to_str()).map(|x|x.eq_ignore_ascii_case("jar")).unwrap_or(false){let meta=entry.metadata().map_err(|e|e.to_string())?;let file_name=entry.file_name().to_string_lossy().into_owned();items.push(InstalledAddon{name:file_name.trim_end_matches(".jar").to_owned(),file_name,size:meta.len(),kind:kind.clone()});}}items.sort_by(|a,b|a.name.to_lowercase().cmp(&b.name.to_lowercase()));Ok(items)}
#[tauri::command]
async fn search_modrinth_addons(query:String,kind:String,server_id:String,state:State<'_,AppState>)->Result<Vec<ModrinthProject>,String>{if !["plugin","mod"].contains(&kind.as_str()){return Err("Addon type must be plugin or mod".into());}let(directory,jar)=server_profile_for_addons(&state,&server_id)?;let runtime=detect_runtime(&directory,&jar);if !runtime.supported||runtime.addon_kind!=kind{return Err(format!("{} does not support {} installation",runtime.software,if kind=="plugin"{"plugins"}else{"mods"}));}let mut facets=vec![vec![format!("project_type:{}",kind)],vec![format!("categories:{}",runtime.loader)]];if let Some(version)=runtime.minecraft_version.as_ref(){facets.push(vec![format!("versions:{version}")]);}let client=reqwest::Client::new();let response=client.get("https://api.modrinth.com/v2/search").query(&[("query",query.trim()),("limit","24"),("facets",&serde_json::to_string(&facets).map_err(|e|e.to_string())?)]).header("User-Agent","CraftPilot/0.7.0 (https://github.com/Demonariya/CraftPilot)").send().await.map_err(|e|format!("Modrinth search failed: {e}"))?;if !response.status().is_success(){return Err(format!("Modrinth returned HTTP {}",response.status()));}let value=response.json::<Value>().await.map_err(|e|e.to_string())?;let hits=value.get("hits").and_then(Value::as_array).ok_or("Modrinth search response is invalid")?;Ok(hits.iter().filter_map(|hit|Some(ModrinthProject{project_id:hit.get("project_id")?.as_str()?.to_owned(),title:hit.get("title")?.as_str()?.to_owned(),description:hit.get("description").and_then(Value::as_str).unwrap_or("").to_owned(),slug:hit.get("slug").and_then(Value::as_str).unwrap_or("").to_owned(),downloads:hit.get("downloads").and_then(Value::as_u64).unwrap_or(0),icon_url:hit.get("icon_url").and_then(Value::as_str).map(str::to_owned),categories:hit.get("categories").and_then(Value::as_array).map(|a|a.iter().filter_map(Value::as_str).map(str::to_owned).collect()).unwrap_or_default()})).collect())}
#[tauri::command]
async fn install_modrinth_addon(server_id:String,project_id:String,kind:String,state:State<'_,AppState>)->Result<String,String>{if !["plugin","mod"].contains(&kind.as_str()){return Err("Invalid addon type".into());}let(directory,jar)=server_profile_for_addons(&state,&server_id)?;let runtime=detect_runtime(&directory,&jar);if !runtime.supported||runtime.addon_kind!=kind{return Err(format!("{} does not support this addon type",runtime.software));}{let processes=state.processes.lock().await;if processes.contains_key(&server_id){return Err("Stop the server before installing or updating addons".into());}}if !project_id.chars().all(|c|c.is_ascii_alphanumeric()||c=='-'){return Err("Invalid Modrinth project ID".into());}let mut request=reqwest::Client::new().get(format!("https://api.modrinth.com/v2/project/{project_id}/version")).header("User-Agent","CraftPilot/0.7.0 (https://github.com/Demonariya/CraftPilot)");let loaders=serde_json::to_string(&vec![runtime.loader.as_str()]).map_err(|e|e.to_string())?;request=request.query(&[("loaders",loaders.as_str())]);if let Some(version)=runtime.minecraft_version.as_ref(){let versions=serde_json::to_string(&vec![version]).map_err(|e|e.to_string())?;request=request.query(&[("game_versions",versions.as_str())]);}let response=request.send().await.map_err(|e|format!("Could not fetch compatible versions: {e}"))?;if !response.status().is_success(){return Err(format!("Modrinth version lookup returned HTTP {}",response.status()));}let versions=response.json::<Value>().await.map_err(|e|e.to_string())?;let version=versions.as_array().and_then(|a|a.first()).ok_or("No compatible release was found for this server version and loader")?;let files=version.get("files").and_then(Value::as_array).ok_or("Modrinth release has no downloadable files")?;let file=files.iter().find(|f|f.get("primary").and_then(Value::as_bool)==Some(true)).or_else(||files.first()).ok_or("Modrinth release has no files")?;let filename=file.get("filename").and_then(Value::as_str).ok_or("Modrinth filename is missing")?;if PathBuf::from(filename).file_name().and_then(|n|n.to_str())!=Some(filename)||!filename.to_lowercase().ends_with(".jar"){return Err("Modrinth returned an unsafe or unsupported addon filename".into());}let url=file.get("url").and_then(Value::as_str).ok_or("Modrinth download URL is missing")?;let parsed=reqwest::Url::parse(url).map_err(|e|e.to_string())?;if parsed.host_str()!=Some("cdn.modrinth.com")||parsed.scheme()!="https"{return Err("Download host is not an approved Modrinth CDN".into());}let download=reqwest::Client::new().get(parsed).header("User-Agent","CraftPilot/0.7.0").send().await.map_err(|e|format!("Addon download failed: {e}"))?;if !download.status().is_success(){return Err(format!("Addon download returned HTTP {}",download.status()));}let bytes=download.bytes().await.map_err(|e|e.to_string())?;if bytes.is_empty()||bytes.len()>100*1024*1024{return Err("Addon download is empty or exceeds the 100 MB safety limit".into());}let root=PathBuf::from(directory);let folder=root.join(if kind=="plugin"{"plugins"}else{"mods"});fs::create_dir_all(&folder).map_err(|e|e.to_string())?;let destination=folder.join(filename);if destination.exists(){return Err("An addon with this filename already exists. Remove it before installing another version.".into());}let temp=folder.join(format!("{filename}.part"));fs::write(&temp,&bytes).map_err(|e|e.to_string())?;if let Err(e)=fs::rename(&temp,&destination){let _=fs::remove_file(temp);return Err(format!("Could not finalize addon installation: {e}"));}Ok(filename.to_owned())}
#[tauri::command]
async fn remove_server_addon(server_id:String,kind:String,file_name:String,state:State<'_,AppState>)->Result<(),String>{if !["plugin","mod"].contains(&kind.as_str()){return Err("Invalid addon type".into());}if PathBuf::from(&file_name).file_name().and_then(|n|n.to_str())!=Some(file_name.as_str())||!file_name.to_lowercase().ends_with(".jar"){return Err("Invalid addon filename".into());}let(directory,jar)=server_profile_for_addons(&state,&server_id)?;let runtime=detect_runtime(&directory,&jar);if runtime.addon_kind!=kind{return Err(format!("{} does not support this addon type",runtime.software));}{let processes=state.processes.lock().await;if processes.contains_key(&server_id){return Err("Stop the server before removing addons".into());}}let root=PathBuf::from(directory).canonicalize().map_err(|e|e.to_string())?;let folder=root.join(if kind=="plugin"{"plugins"}else{"mods"});let path=folder.join(file_name);let canonical=path.canonicalize().map_err(|e|format!("Addon file not found: {e}"))?;if !canonical.starts_with(&root)||!canonical.is_file(){return Err("Addon path is outside the server directory".into());}fs::remove_file(canonical).map_err(|e|format!("Could not remove addon: {e}"))}

#[derive(Serialize)]
struct ServerFileEntry { name:String, relative_path:String, is_directory:bool, size:u64 }
fn server_root(state:&State<'_,AppState>,server_id:&str)->Result<PathBuf,String>{let conn=db_conn(state)?;let dir:String=conn.query_row("SELECT directory FROM servers WHERE id=?1",[server_id],|r|r.get(0)).map_err(|e|format!("Server profile not found: {e}"))?;PathBuf::from(dir).canonicalize().map_err(|e|format!("Server directory is unavailable: {e}"))}
fn checked_server_path(root:&PathBuf,relative_path:&str)->Result<PathBuf,String>{let candidate=if relative_path.trim().is_empty(){root.clone()}else{root.join(relative_path)};let path=candidate.canonicalize().map_err(|e|format!("Cannot access path: {e}"))?;if !path.starts_with(root){return Err("Access outside the server directory is blocked".into());}Ok(path)}
#[tauri::command]
fn list_server_files(server_id:String,relative_path:Option<String>,state:State<'_,AppState>)->Result<Vec<ServerFileEntry>,String>{let root=server_root(&state,&server_id)?;let current=checked_server_path(&root,relative_path.as_deref().unwrap_or(""))?;if !current.is_dir(){return Err("Selected path is not a directory".into());}let mut entries=Vec::new();for entry in fs::read_dir(&current).map_err(|e|e.to_string())?{let entry=entry.map_err(|e|e.to_string())?;let path=entry.path();let canonical=path.canonicalize().map_err(|e|e.to_string())?;if !canonical.starts_with(&root){continue;}let metadata=entry.metadata().map_err(|e|e.to_string())?;let relative=canonical.strip_prefix(&root).map_err(|e|e.to_string())?.to_string_lossy().replace(std::path::MAIN_SEPARATOR,"/");entries.push(ServerFileEntry{name:entry.file_name().to_string_lossy().into_owned(),relative_path:relative,is_directory:metadata.is_dir(),size:metadata.len()});}entries.sort_by(|a,b|b.is_directory.cmp(&a.is_directory).then_with(||a.name.to_lowercase().cmp(&b.name.to_lowercase())));Ok(entries)}
#[tauri::command]
fn read_server_file(server_id:String,relative_path:String,state:State<'_,AppState>)->Result<String,String>{let root=server_root(&state,&server_id)?;let path=checked_server_path(&root,&relative_path)?;if !path.is_file(){return Err("Selected path is not a file".into());}let metadata=fs::metadata(&path).map_err(|e|e.to_string())?;if metadata.len()>1_048_576{return Err("Files larger than 1 MB cannot be opened in the text editor".into());}fs::read_to_string(path).map_err(|e|format!("File is not valid UTF-8 text or cannot be read: {e}"))}
#[tauri::command]
fn write_server_file(server_id:String,relative_path:String,content:String,state:State<'_,AppState>)->Result<(),String>{if content.len()>1_048_576{return Err("Text files are limited to 1 MB".into());}let root=server_root(&state,&server_id)?;let relative=PathBuf::from(&relative_path);if relative.is_absolute(){return Err("Absolute paths are not allowed".into());}let name=relative.file_name().ok_or("Invalid file name")?;let parent_rel=relative.parent().unwrap_or(std::path::Path::new(""));let parent=checked_server_path(&root,&parent_rel.to_string_lossy())?;if !parent.is_dir(){return Err("Parent directory does not exist".into());}let destination=parent.join(name);if destination.exists(){let existing=destination.canonicalize().map_err(|e|e.to_string())?;if !existing.starts_with(&root)||!existing.is_file(){return Err("Target file is outside the server directory or is not a file".into());}}fs::write(destination,content).map_err(|e|format!("Could not save file: {e}"))}


#[derive(Serialize, Clone)]
struct BackupRecord { id:String, server_id:String, label:String, created_at:String, size_bytes:u64 }
fn copy_backup_tree(source:&std::path::Path,destination:&std::path::Path)->Result<u64,String>{
 fs::create_dir_all(destination).map_err(|e|e.to_string())?;let mut total=0u64;
 for entry in fs::read_dir(source).map_err(|e|e.to_string())?{let entry=entry.map_err(|e|e.to_string())?;let src=entry.path();let dst=destination.join(entry.file_name());let meta=fs::symlink_metadata(&src).map_err(|e|e.to_string())?;
  if meta.file_type().is_symlink(){return Err(format!("Backup stopped: symbolic links are not supported ({})",src.display()));}
  if meta.is_dir(){total=total.saturating_add(copy_backup_tree(&src,&dst)?);}else if meta.is_file(){fs::copy(&src,&dst).map_err(|e|format!("Could not copy {}: {e}",src.display()))?;total=total.saturating_add(meta.len());}
 }Ok(total)
}
async fn create_backup_inner(server_id:&str,state:&State<'_,AppState>,label:&str)->Result<BackupRecord,String>{
 {let processes=state.processes.lock().await;if processes.contains_key(server_id){return Err("Stop the server before creating a backup".into());}}
 let (directory,name)={let conn=db_conn(state)?;conn.query_row("SELECT directory,name FROM servers WHERE id=?1",[server_id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).map_err(|e|format!("Server profile not found: {e}"))?};
 let root=PathBuf::from(directory).canonicalize().map_err(|e|format!("Server directory is unavailable: {e}"))?;
 let id=Uuid::new_v4().to_string();let destination=state.backup_root.join(server_id).join(&id);let parent=destination.parent().ok_or("Invalid backup destination")?;fs::create_dir_all(parent).map_err(|e|e.to_string())?;
 let size=match copy_backup_tree(&root,&destination){Ok(n)=>n,Err(e)=>{let _=fs::remove_dir_all(&destination);return Err(e)}};
 let created=Utc::now().to_rfc3339();let record=BackupRecord{id:id.clone(),server_id:server_id.to_owned(),label:label.to_owned(),created_at:created.clone(),size_bytes:size};
 let conn=db_conn(state)?;if let Err(e)=conn.execute("INSERT INTO backups(id,server_id,label,created_at,size_bytes,path) VALUES(?1,?2,?3,?4,?5,?6)",params![id,server_id,label,created,size.min(i64::MAX as u64) as i64,destination.to_string_lossy()]){let _=fs::remove_dir_all(destination);return Err(e.to_string());}let _=name;Ok(record)
}
#[tauri::command]
async fn create_server_backup(server_id:String,state:State<'_,AppState>)->Result<BackupRecord,String>{create_backup_inner(&server_id,&state,"Manual backup").await}
#[tauri::command]
fn list_server_backups(server_id:String,state:State<'_,AppState>)->Result<Vec<BackupRecord>,String>{let conn=db_conn(&state)?;let mut stmt=conn.prepare("SELECT id,server_id,label,created_at,size_bytes FROM backups WHERE server_id=?1 ORDER BY created_at DESC").map_err(|e|e.to_string())?;let rows=stmt.query_map([server_id],|r|Ok(BackupRecord{id:r.get(0)?,server_id:r.get(1)?,label:r.get(2)?,created_at:r.get(3)?,size_bytes:r.get::<_,i64>(4)?.max(0) as u64})).map_err(|e|e.to_string())?;rows.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())}
#[tauri::command]
async fn restore_server_backup(server_id:String,backup_id:String,state:State<'_,AppState>)->Result<(),String>{
 {let processes=state.processes.lock().await;if processes.contains_key(&server_id){return Err("Stop the server before restoring a backup".into());}}
 let (directory,stored_path)={let conn=db_conn(&state)?;let directory:String=conn.query_row("SELECT directory FROM servers WHERE id=?1",[&server_id],|r|r.get(0)).map_err(|e|e.to_string())?;let path:String=conn.query_row("SELECT path FROM backups WHERE id=?1 AND server_id=?2",params![backup_id,server_id],|r|r.get(0)).map_err(|e|format!("Backup not found: {e}"))?;(directory,path)};
 let backup_root=state.backup_root.canonicalize().map_err(|e|e.to_string())?;let backup=PathBuf::from(stored_path).canonicalize().map_err(|e|e.to_string())?;if !backup.starts_with(&backup_root)||!backup.is_dir(){return Err("Backup path failed the safety check".into());}
 let _safety=create_backup_inner(&server_id,&state,"Automatic pre-restore safety backup").await?;
 let root=PathBuf::from(directory).canonicalize().map_err(|e|e.to_string())?;let parent=root.parent().ok_or("Cannot restore a server folder without a parent directory")?;let token=Uuid::new_v4().to_string();let staging=parent.join(format!(".craftpilot-restore-{token}"));let previous=parent.join(format!(".craftpilot-previous-{token}"));
 if let Err(e)=copy_backup_tree(&backup,&staging){let _=fs::remove_dir_all(&staging);return Err(format!("Could not prepare restore: {e}"));}
 if let Err(e)=fs::rename(&root,&previous){let _=fs::remove_dir_all(&staging);return Err(format!("Could not move the current server folder: {e}"));}
 if let Err(e)=fs::rename(&staging,&root){let rollback=fs::rename(&previous,&root);let _=fs::remove_dir_all(&staging);return Err(format!("Could not activate restored files: {e}; rollback: {rollback:?}"));}
 let _=fs::remove_dir_all(previous);Ok(())
}
#[tauri::command]
fn delete_server_backup(server_id:String,backup_id:String,state:State<'_,AppState>)->Result<(),String>{let stored:String={let conn=db_conn(&state)?;conn.query_row("SELECT path FROM backups WHERE id=?1 AND server_id=?2",params![backup_id,server_id],|r|r.get(0)).map_err(|e|format!("Backup not found: {e}"))?};let root=state.backup_root.canonicalize().map_err(|e|e.to_string())?;let path=PathBuf::from(stored).canonicalize().map_err(|e|e.to_string())?;if !path.starts_with(&root)||!path.is_dir(){return Err("Backup path failed the safety check".into());}fs::remove_dir_all(path).map_err(|e|format!("Could not delete backup: {e}"))?;let conn=db_conn(&state)?;conn.execute("DELETE FROM backups WHERE id=?1 AND server_id=?2",params![backup_id,server_id]).map_err(|e|e.to_string())?;Ok(())}


#[derive(Serialize, Clone)]
struct BackupSchedule { id:String, server_id:String, interval_minutes:u32, retention_count:u32, enabled:bool, next_run_at:Option<String>, last_run_at:Option<String>, last_error:Option<String> }
#[tauri::command]
fn list_backup_schedules(server_id:String,state:State<'_,AppState>)->Result<Vec<BackupSchedule>,String>{let conn=db_conn(&state)?;let mut stmt=conn.prepare("SELECT id,server_id,interval_minutes,retention_count,enabled,next_run_at,last_run_at,last_error FROM backup_schedules WHERE server_id=?1 ORDER BY id").map_err(|e|e.to_string())?;let rows=stmt.query_map([server_id],|r|Ok(BackupSchedule{id:r.get(0)?,server_id:r.get(1)?,interval_minutes:r.get::<_,i64>(2)?.max(0) as u32,retention_count:r.get::<_,i64>(3)?.max(1) as u32,enabled:r.get::<_,i64>(4)?!=0,next_run_at:r.get(5)?,last_run_at:r.get(6)?,last_error:r.get(7)?})).map_err(|e|e.to_string())?;rows.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())}
#[tauri::command]
fn save_backup_schedule(server_id:String,interval_minutes:u32,retention_count:u32,enabled:bool,state:State<'_,AppState>)->Result<BackupSchedule,String>{if !(15..=10080).contains(&interval_minutes){return Err("Backup interval must be between 15 minutes and 7 days".into());}if !(1..=100).contains(&retention_count){return Err("Retention must be between 1 and 100 backups".into());}let id={let conn=db_conn(&state)?;conn.query_row("SELECT id FROM backup_schedules WHERE server_id=?1",[&server_id],|r|r.get::<_,String>(0)).unwrap_or_else(|_|Uuid::new_v4().to_string())};let now=Utc::now();let next=if enabled{Some((now+chrono::Duration::minutes(interval_minutes as i64)).to_rfc3339())}else{None};let conn=db_conn(&state)?;conn.execute("INSERT INTO backup_schedules(id,server_id,interval_minutes,retention_count,enabled,next_run_at,last_run_at,last_error) VALUES(?1,?2,?3,?4,?5,?6,NULL,NULL) ON CONFLICT(server_id) DO UPDATE SET interval_minutes=excluded.interval_minutes,retention_count=excluded.retention_count,enabled=excluded.enabled,next_run_at=excluded.next_run_at,last_error=NULL",params![id,server_id,interval_minutes as i64,retention_count as i64,enabled as i64,next]).map_err(|e|e.to_string())?;conn.query_row("SELECT id,server_id,interval_minutes,retention_count,enabled,next_run_at,last_run_at,last_error FROM backup_schedules WHERE server_id=?1",[server_id],|r|Ok(BackupSchedule{id:r.get(0)?,server_id:r.get(1)?,interval_minutes:r.get::<_,i64>(2)?.max(0) as u32,retention_count:r.get::<_,i64>(3)?.max(1) as u32,enabled:r.get::<_,i64>(4)?!=0,next_run_at:r.get(5)?,last_run_at:r.get(6)?,last_error:r.get(7)?})).map_err(|e|e.to_string())}
#[tauri::command]
fn delete_backup_schedule(server_id:String,state:State<'_,AppState>)->Result<(),String>{let conn=db_conn(&state)?;conn.execute("DELETE FROM backup_schedules WHERE server_id=?1",[server_id]).map_err(|e|e.to_string())?;Ok(())}
fn prune_server_backups(state:&State<'_,AppState>,server_id:&str,keep:u32)->Result<(),String>{let rows={let conn=db_conn(state)?;let mut stmt=conn.prepare("SELECT id,path FROM backups WHERE server_id=?1 ORDER BY created_at DESC,id DESC LIMIT -1 OFFSET ?2").map_err(|e|e.to_string())?;let rows=stmt.query_map(params![server_id,keep as i64],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).map_err(|e|e.to_string())?;rows.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?};let root=state.backup_root.canonicalize().map_err(|e|e.to_string())?;for(id,stored)in rows{let path=PathBuf::from(stored).canonicalize().map_err(|e|e.to_string())?;if !path.starts_with(&root)||!path.is_dir(){return Err("A backup selected for retention cleanup failed path validation".into());}fs::remove_dir_all(path).map_err(|e|format!("Retention cleanup failed: {e}"))?;let conn=db_conn(state)?;conn.execute("DELETE FROM backups WHERE id=?1 AND server_id=?2",params![id,server_id]).map_err(|e|e.to_string())?;}Ok(())}
fn start_backup_scheduler(app:AppHandle){tauri::async_runtime::spawn(async move{let mut ticker=tokio::time::interval(Duration::from_secs(30));loop{ticker.tick().await;let state=app.state::<AppState>();let now=Utc::now().to_rfc3339();let due={let Ok(conn)=state.db.lock()else{continue};let Ok(mut stmt)=conn.prepare("SELECT id,server_id,interval_minutes,retention_count FROM backup_schedules WHERE enabled=1 AND next_run_at IS NOT NULL AND next_run_at<=?1")else{continue};let Ok(rows)=stmt.query_map([&now],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?)))else{continue};rows.filter_map(Result::ok).collect::<Vec<_>>()};for(id,server_id,interval,retention)in due{let result=create_backup_inner(&server_id,&state,"Scheduled backup").await.and_then(|_|prune_server_backups(&state,&server_id,retention.max(1) as u32));let next=match &result{Ok(())=>Utc::now()+chrono::Duration::minutes(interval.max(15)),Err(_)=>Utc::now()+chrono::Duration::minutes(5)};let last=if result.is_ok(){Some(Utc::now().to_rfc3339())}else{None};let err=result.err();if let Ok(conn)=state.db.lock(){let _=conn.execute("UPDATE backup_schedules SET next_run_at=?1,last_run_at=COALESCE(?2,last_run_at),last_error=?3 WHERE id=?4",params![next.to_rfc3339(),last,err,id]);}}}});}

pub fn run(){tauri::Builder::default().plugin(tauri_plugin_dialog::init()).setup(|app|{let app_data=app.path().app_data_dir()?;fs::create_dir_all(&app_data)?;let backup_root=app_data.join("backups");fs::create_dir_all(&backup_root)?;let conn=Connection::open(app_data.join("craftpilot.db"))?;conn.execute_batch("PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS servers(id TEXT PRIMARY KEY,name TEXT NOT NULL,directory TEXT NOT NULL,jar_file TEXT NOT NULL,java_path TEXT NOT NULL,min_memory_mb INTEGER NOT NULL,max_memory_mb INTEGER NOT NULL,created_at TEXT NOT NULL);CREATE TABLE IF NOT EXISTS online_players(server_id TEXT NOT NULL,player_name TEXT NOT NULL,joined_at TEXT NOT NULL,PRIMARY KEY(server_id,player_name));CREATE TABLE IF NOT EXISTS player_sessions(id INTEGER PRIMARY KEY AUTOINCREMENT,server_id TEXT NOT NULL,player_name TEXT NOT NULL,joined_at TEXT NOT NULL,left_at TEXT);CREATE INDEX IF NOT EXISTS idx_player_sessions_server ON player_sessions(server_id,id DESC);CREATE TABLE IF NOT EXISTS backups(id TEXT PRIMARY KEY,server_id TEXT NOT NULL,label TEXT NOT NULL,created_at TEXT NOT NULL,size_bytes INTEGER NOT NULL,path TEXT NOT NULL);CREATE INDEX IF NOT EXISTS idx_backups_server ON backups(server_id,created_at DESC);CREATE TABLE IF NOT EXISTS backup_schedules(id TEXT PRIMARY KEY,server_id TEXT NOT NULL UNIQUE,interval_minutes INTEGER NOT NULL,retention_count INTEGER NOT NULL,enabled INTEGER NOT NULL,next_run_at TEXT,last_run_at TEXT,last_error TEXT);CREATE INDEX IF NOT EXISTS idx_backup_schedules_due ON backup_schedules(enabled,next_run_at);")?;app.manage(AppState{db:Mutex::new(conn),processes:tokio::sync::Mutex::new(HashMap::new()),backup_root});app.manage(InstallerState::default());start_backup_scheduler(app.handle().clone());Ok(())}).invoke_handler(tauri::generate_handler![list_servers,register_server,start_server,stop_server,send_server_command,get_host_metrics,list_installable_versions,install_server,cancel_server_install,list_server_files,read_server_file,write_server_file,get_server_players,get_player_history,refresh_server_players,manage_player,detect_server_runtime,list_server_addons,search_modrinth_addons,install_modrinth_addon,remove_server_addon,create_server_backup,list_server_backups,restore_server_backup,delete_server_backup,list_backup_schedules,save_backup_schedule,delete_backup_schedule,list_java_runtimes,set_server_java,remove_managed_java,install_java_runtime,get_java_recommendation]).run(tauri::generate_context!()).expect("error while running CraftPilot");}
