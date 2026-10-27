import { useEffect } from "react";
import { clearKafkaReloadRoute, type KafkaReloadRoute, readKafkaReloadRoute } from "./kafka-navigation";

interface KafkaReloadRouteRestorerProps {
  onRestoreRoute: (route: KafkaReloadRoute) => void;
}

export type RestoreStep = "done" | "navigate" | "give-up";

/**
 * What the restorer does at one of its attempts: done once the frame shows the saved page; give up once the frame left
 * the page it started on for another one, which means the user went somewhere else first, or after its last attempt;
 * otherwise navigate to the saved page (again: the host may not have registered the pages yet).
 */
export function nextRestoreStep(input: {
  pathname: string;
  startPathname: string;
  pageId: string;
  attempts: number;
  maxAttempts: number;
}): RestoreStep {
  if (input.pathname.endsWith(`/${input.pageId}`)) return "done";
  if (input.pathname !== input.startPathname) return "give-up";
  return input.attempts >= input.maxAttempts ? "give-up" : "navigate";
}

const MAX_ATTEMPTS = 100;

export function KafkaReloadRouteRestorer({ onRestoreRoute }: KafkaReloadRouteRestorerProps) {
  useEffect(() => {
    const route = readKafkaReloadRoute();
    if (!route) return;

    const startPathname = window.location.pathname;
    let attempts = 0;
    let restoreTimer: number | undefined;
    const stop = () => {
      if (restoreTimer !== undefined) window.clearInterval(restoreTimer);
    };
    const restore = () => {
      const step = nextRestoreStep({
        pathname: window.location.pathname,
        startPathname,
        pageId: route.pageId,
        attempts,
        maxAttempts: MAX_ATTEMPTS,
      });

      if (step !== "navigate") {
        if (step === "done" || window.location.pathname !== startPathname) clearKafkaReloadRoute();
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
