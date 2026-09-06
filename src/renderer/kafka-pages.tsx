import { Renderer } from "@freelensapp/extensions";
import { useEffect, useState } from "react";
import { updateKafkaClusterQuery } from "./kafka-navigation";
import { KafkaOverviewPage, type KafkaOverviewPageProps } from "./kafka-overview";

interface KafkaClustersPageParams {
  query: string;
}

export interface KafkaClustersPageProps extends Omit<KafkaOverviewPageProps, "query" | "onQueryChange"> {
  params?: {
    query: Renderer.Navigation.PageParam<KafkaClustersPageParams["query"]>;
  };
}

export function KafkaClustersPage({ params, ...props }: KafkaClustersPageProps) {
  const routeQuery = params?.query.get() ?? "";
  const [query, setQuery] = useState(routeQuery);

  useEffect(() => setQuery(routeQuery), [routeQuery]);

  useEffect(() => {
    const syncFromHistory = () => setQuery(params?.query.get() ?? "");
    window.addEventListener("popstate", syncFromHistory);
    return () => window.removeEventListener("popstate", syncFromHistory);
  }, [params?.query]);

  return (
    <KafkaOverviewPage
      {...props}
      query={query}
      onQueryChange={(nextQuery) => {
        setQuery(nextQuery);
        updateKafkaClusterQuery(params?.query, nextQuery);
      }}
    />
  );
}
