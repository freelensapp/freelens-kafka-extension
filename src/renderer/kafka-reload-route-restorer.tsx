import { useEffect } from "react";
import { clearKafkaReloadRoute, type KafkaReloadRoute, readKafkaReloadRoute } from "./kafka-navigation";

interface KafkaReloadRouteRestorerProps {
  onRestoreRoute: (route: KafkaReloadRoute) => void;
}

export function KafkaReloadRouteRestorer({ onRestoreRoute }: KafkaReloadRouteRestorerProps) {
  useEffect(() => {
    const route = readKafkaReloadRoute();
    if (!route) return;

    let attempts = 0;
    let restoreTimer: number | undefined;
    const stop = () => {
      if (restoreTimer !== undefined) window.clearInterval(restoreTimer);
    };
    const restore = () => {
      if (window.location.pathname.endsWith(`/${route.pageId}`)) {
        clearKafkaReloadRoute();
        stop();
        return;
      }
      if (attempts >= 100) {
        stop();
        return;
      }
      attempts += 1;
      onRestoreRoute(route);
    };

    restore();
    restoreTimer = window.setInterval(restore, 100);
    return stop;
  }, [onRestoreRoute]);

  return null;
}
