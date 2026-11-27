use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, time::Duration};
use sysinfo::System;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, process::{Child, ChildStdin, Command}, time::sleep};
use uuid::Uuid;
use futures_util::StreamExt;
use serde_json::Value;

struct AppState { db: Mutex<Connection>, processes: tokio::sync::Mutex<HashMap<String, RunningServer>> }
struct RunningServer { child: Child, stdin: ChildStdin }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerProfile { id:String, name:String, directory:String, jar_file:String, java_path:String, min_memory_mb:u32, max_memory_mb:u32, created_at:String, status:String }
#[derive(Serialize)]
struct ServerOutput { server_id:String, stream:String, line:String, timestamp:String }
#[derive(Serialize)]
struct HostMetrics { cpu_usage:f32, used_memory_mb:u64, total_memory_mb:u64 }
fn emit_line(app:&AppHandle, server_id:&str, stream:&str, line:String) { let payload=ServerOutput{server_id:server_id.into(),stream:stream.into(),line,timestamp:Utc::now().to_rfc3339()};let _=app.emit("server://output",payload); }
fn db_conn(state:&State<'_,AppState>)->Result<std::sync::MutexGuard<'_,Connection>,String>{state.db.lock().map_err(|e|format!("Database lock failed: {e}"))}

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
 let out_app=app.clone();let out_id=server_id.clone();tauri::async_runtime::spawn(async move{let mut lines=BufReader::new(stdout).lines();while let Ok(Some(line))=lines.next_line().await{emit_line(&out_app,&out_id,"stdout",line);}});
 let err_app=app.clone();let err_id=server_id.clone();tauri::async_runtime::spawn(async move{let mut lines=BufReader::new(stderr).lines();while let Ok(Some(line))=lines.next_line().await{emit_line(&err_app,&err_id,"stderr",line);}});
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

pub fn run(){tauri::Builder::default().plugin(tauri_plugin_dialog::init()).setup(|app|{let app_data=app.path().app_data_dir()?;fs::create_dir_all(&app_data)?;let conn=Connection::open(app_data.join("craftpilot.db"))?;conn.execute_batch("PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS servers(id TEXT PRIMARY KEY,name TEXT NOT NULL,directory TEXT NOT NULL,jar_file TEXT NOT NULL,java_path TEXT NOT NULL,min_memory_mb INTEGER NOT NULL,max_memory_mb INTEGER NOT NULL,created_at TEXT NOT NULL);")?;app.manage(AppState{db:Mutex::new(conn),processes:tokio::sync::Mutex::new(HashMap::new())});app.manage(InstallerState::default());Ok(())}).invoke_handler(tauri::generate_handler![list_servers,register_server,start_server,stop_server,send_server_command,get_host_metrics,list_installable_versions,install_server,cancel_server_install,list_server_files,read_server_file,write_server_file]).run(tauri::generate_context!()).expect("error while running CraftPilot");}
