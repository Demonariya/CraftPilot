export type ServerStatus = "stopped" | "starting" | "running" | "stopping" | "unknown";
export interface ServerProfile { id:string; name:string; directory:string; jar_file:string; java_path:string; min_memory_mb:number; max_memory_mb:number; jvm_args:string; shutdown_timeout_secs:number; auto_start:boolean; restart_on_crash:boolean; restart_delay_secs:number; max_restart_attempts:number; created_at:string; status:ServerStatus; }
export interface ServerOutput { server_id:string; stream:"stdout"|"stderr"|"system"; line:string; timestamp:string; }
export interface HostMetrics { cpu_usage:number; used_memory_mb:number; total_memory_mb:number; }

export interface ServerMetrics { running:boolean; cpu_usage_percent:number; memory_mb:number; player_count:number; uptime_secs:number; }

export interface RemoteHost { id:string; name:string; host:string; port:number; username:string; identity_file:string; created_at:string; }
export interface RemoteCommandResult { stdout:string; stderr:string; exit_code:number; }

export interface RemoteServer { id:string; remote_host_id:string; name:string; working_directory:string; jar_path:string; java_path:string; min_memory_mb:number; max_memory_mb:number; session_name:string; created_at:string; }
