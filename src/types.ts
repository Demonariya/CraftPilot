export type ServerStatus = "stopped" | "starting" | "running" | "stopping" | "unknown";
export interface ServerProfile { id:string; name:string; directory:string; jar_file:string; java_path:string; min_memory_mb:number; max_memory_mb:number; created_at:string; status:ServerStatus; }
export interface ServerOutput { server_id:string; stream:"stdout"|"stderr"|"system"; line:string; timestamp:string; }
export interface HostMetrics { cpu_usage:number; used_memory_mb:number; total_memory_mb:number; }
