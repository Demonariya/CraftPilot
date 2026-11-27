use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::PathBuf, sync::Mutex, time::Duration};
use sysinfo::System;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, process::{Child, ChildStdin, Command}, time::sleep};
use uuid::Uuid;

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

pub fn run(){tauri::Builder::default().plugin(tauri_plugin_dialog::init()).setup(|app|{let app_data=app.path().app_data_dir()?;fs::create_dir_all(&app_data)?;let conn=Connection::open(app_data.join("craftpilot.db"))?;conn.execute_batch("PRAGMA journal_mode=WAL;CREATE TABLE IF NOT EXISTS servers(id TEXT PRIMARY KEY,name TEXT NOT NULL,directory TEXT NOT NULL,jar_file TEXT NOT NULL,java_path TEXT NOT NULL,min_memory_mb INTEGER NOT NULL,max_memory_mb INTEGER NOT NULL,created_at TEXT NOT NULL);")?;app.manage(AppState{db:Mutex::new(conn),processes:tokio::sync::Mutex::new(HashMap::new())});Ok(())}).invoke_handler(tauri::generate_handler![list_servers,register_server,start_server,stop_server,send_server_command,get_host_metrics]).run(tauri::generate_context!()).expect("error while running CraftPilot");}
