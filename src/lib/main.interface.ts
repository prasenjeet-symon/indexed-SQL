// Types
export type result_callback_interface = (result: string[]) => void;
export type arithmatic_operator = "/" | "*" | "+" | "-";
export type comparision_operator = "=" | "!=" | "<" | ">" | "<=" | ">=";
export type generalTrimOption = "{" | "[" | "(";
export type join_type = "INNER JOIN" | "LEFT OUTER JOIN" | "RIGHT OUTER JOIN";

export interface comparision_cell_interface {
  left_operand: string | null;
  right_operand: string | null;
  operator: comparision_operator;
  result?: boolean;
}

export interface group_cell_interface {
  value: string[];
  result?: boolean;
}

export interface comparision_operands_interface {
  value: string[];
  operator: string;
  result?: any;
}

export interface table_column_info {
  column_name: string;
  column_datatype: { first_part: string; second_part: string[] };
  can_null: boolean;
  is_unsigned: boolean;
  default_value: "null" | string;
  is_primary_key: boolean;
  is_auto_increment: boolean;
}

export interface databaseConnectionConfig {
  database_name: string;
  multi_query?: boolean;
}
