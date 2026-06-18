import { useEffect, useRef, useState } from "react";

type SSEStatus = "connecting" | "open" | "closed" | "error";

interface UseSSEResult<T> {
  lastMessage: T | null;
  status: SSEStatus;
}

/**
 * useSSE — subscribes to a Server-Sent Events endpoint and returns the
 * most-recent parsed JSON message plus the connection status.
 *
 * Automatically reconnects with a 3-second back-off whenever the stream
 * closes unexpectedly.
 */
export function useSSE<T>(url: string): UseSSEResult<T> {
  const [lastMessage, setLastMessage] = useState<T | null>(null);
  const [status, setStatus] = useState<SSEStatus>("connecting");
  const esRef = useRef<EventSource | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    let cancelled = false;

    function connect() {
      if (cancelled) return;

      setStatus("connecting");
      const es = new EventSource(url);
      esRef.current = es;

      es.onopen = () => {
        if (!cancelled) setStatus("open");
      };

      es.onmessage = (event) => {
        if (cancelled) return;
        try {
          const data = JSON.parse(event.data) as T;
          setLastMessage(data);
        } catch {
          // non-JSON heartbeat / comment — ignore
        }
      };

      es.onerror = () => {
        if (cancelled) return;
        setStatus("error");
        es.close();
        // Reconnect after 3 s
        timerRef.current = setTimeout(connect, 3_000);
      };
    }

    connect();

    return () => {
      cancelled = true;
      esRef.current?.close();
      if (timerRef.current) clearTimeout(timerRef.current);
      setStatus("closed");
    };
  }, [url]);

  return { lastMessage, status };
}
