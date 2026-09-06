export const KAFKA_LIST_PAGE_SIZE = 100;

export interface KafkaListWindow<T> {
  items: T[];
  page: number;
  pageCount: number;
  total: number;
}

export function kafkaListWindow<T>(items: T[], page: number, pageSize = KAFKA_LIST_PAGE_SIZE): KafkaListWindow<T> {
  const safePageSize = Math.max(1, pageSize);
  const pageCount = Math.max(1, Math.ceil(items.length / safePageSize));
  const safePage = Math.min(Math.max(0, page), pageCount - 1);
  return {
    items: items.slice(safePage * safePageSize, (safePage + 1) * safePageSize),
    page: safePage,
    pageCount,
    total: items.length,
  };
}
