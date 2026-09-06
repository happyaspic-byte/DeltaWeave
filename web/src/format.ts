export const bytes = (value: number): string => {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(
    Math.floor(Math.log(value) / Math.log(1024)),
    units.length - 1,
  );
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: i > 0 ? 1 : 0 }).format(value / 1024 ** i)} ${units[i]}`;
};
export const number = (value: number) =>
  new Intl.NumberFormat("ko-KR").format(value);
export const date = (value: number | null | undefined) =>
  value == null
    ? "아직 기록 없음"
    : new Date(value).toLocaleString("ko-KR", {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
export const time = (value: number) =>
  new Date(value).toLocaleTimeString("ko-KR", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
export const ago = (value: number | null | undefined) => {
  if (value == null) return "아직 기록 없음";
  const seconds = Math.max(0, Math.floor((Date.now() - value) / 1000));
  if (seconds < 60) return "방금 전";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}분 전`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}시간 전`;
  return `${Math.floor(seconds / 86400)}일 전`;
};
export const uptime = (seconds: number) =>
  seconds < 3600
    ? `${Math.floor(seconds / 60)}m`
    : `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
export const errorMessage = (error: unknown) =>
  error instanceof Error
    ? error.message
    : "요청을 처리하지 못했습니다. 다시 시도해 주세요.";
