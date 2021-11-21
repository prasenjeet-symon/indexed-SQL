import BigNumber from "bignumber.js";
import {
  comparision_cell_interface,
  comparision_operands_interface,
  comparision_operator,
  group_cell_interface,
  result_callback_interface,
} from "./main.interface";
import { replace_sql_string_with_marker } from "./single-table/select_clause";
import {
  arithmatic_operators_with_modu,
  comparision_operators,
  do_arithmatics,
  find_bracket_close_index,
  found_number_of_comparision_operators,
  is_number,
  is_pure_sql_string,
  logical_operators,
  manage_of,
  resolve_compare,
} from "./utils";

export function make_where_literal(where_string: string) {
  const marker_data = replace_sql_string_with_marker(where_string);

  let normalized_string_arr = marker_data.final_string
    // logical operators
    .replace(/AND/g, " AND ")
    .replace(/OR/g, " OR ")

    // comparision operators
    .replace(/=/g, " = ")
    .replace(/</g, " < ")
    .replace(/>/g, " > ")
    .replace(/<=/g, " <= ")
    .replace(/>=/g, " >= ")
    .replace(/!=/g, " != ")

    // arithmatics operators
    .replace(/\+/g, " + ")
    .replace(/\-/g, " - ")
    .replace(/\*/g, " * ")
    .replace(/\//g, " * ")
    .replace(/\%/g, " * ")

    // bracket ( group maker)
    .replace(/\(/g, " ( ")
    .replace(/\)/g, " ) ")

    // string maker
    .replace(/\'/g, " ' ")
    .replace(/\'/g, " ' ")

    .trim()
    .replace(/\s+/g, " ")
    .split(/\s/g);

  // add the sql string back
  marker_data.marker.forEach((mrk, i) => {
    const marker_string = `MS_${i}`;
    normalized_string_arr = normalized_string_arr.map((p) =>
      p === marker_string ? mrk : p
    );
  });

  return normalized_string_arr;
}

/** Make SQL string */
export function combine_single_quoted_value(where_literals: string[]) {
  const combine_sql_string = (
    where_literals_copy: string[],
    cb: (result: string[]) => void
  ) => {
    let initial_index: any = null;
    let final_index: any = null;

    for (let index = 0; index < where_literals_copy.length; index++) {
      const literal: string = where_literals_copy[index];

      if (literal === "'" && initial_index === null) {
        // found the first single quote of the string
        // set the initial_index to this index
        initial_index = index;
      } else if (literal === "'" && initial_index !== null) {
        // this is matching last single quote of the string.
        // this.represent the end of the string
        // add this index to the final_index
        final_index = index;
      }

      // if there is both final and initial index value in the variable intial_index and final_index
      // then there is string in the array
      // construct the string and add it to the array_literal
      // will modify the array
      // value to replace is  [initial_index , final_index]
      // initial index can we >=0 but final index must be greater than 0
      if (initial_index >= 0 && final_index > 0) {
        // construct the string from the array literal
        const new_string: string = where_literals_copy
          .slice(initial_index + 1, final_index)
          .join(" ");
        // replace the where array with the newly created string
        where_literals_copy.splice(
          initial_index,
          final_index - initial_index + 1,
          `'${new_string}'`
        );
        // move out of this loop
        break;
      }
    }

    // search the where literal to find the string - '
    // if it contain then call this function again
    // if not then task complete
    if (where_literals_copy.includes("'")) {
      // contain the string '
      // there is more string the array
      combine_sql_string(where_literals_copy, cb);
    } else {
      cb(where_literals_copy);
    }
  };

  return new Promise<string[]>((resolve, reject) => {
    combine_sql_string(JSON.parse(JSON.stringify(where_literals)), (result) =>
      resolve(result)
    );
  });
}

export function extract_comparision_operand_left(
  operator_index: number,
  where_literals: string[]
) {
  let break_point_index = null;

  for (let index = operator_index - 1; index >= 0; index--) {
    // if we found "(" then try to find the counter part ")"
    // whole section from "("  to ")"  is part of the operand ( right)
    // set the index to position of the counter part and continue the loop
    const literal = where_literals[index];
    // check for the internal bracket
    if (literal === ")") {
      // found the initial bracket
      // try to find the counter part
      const index_of_counterpart = find_bracket_close_index(
        where_literals,
        "left",
        index - 1
      );
      if (index_of_counterpart > -1) {
        // found the counter part
        // continue the loop
        index = index_of_counterpart;
      } else {
        // not found the counter part
        throw new Error(
          'SQL syntax error left | ")" cannot be found. You missed to put ")".'
        );
      }
    }

    if (
      literal === "(" ||
      literal.toUpperCase() === "AND" ||
      literal.toUpperCase() === "OR"
    ) {
      break_point_index = index;
      break;
    }
  }

  if (break_point_index !== null) {
    const left_operand = where_literals.slice(
      break_point_index + 1,
      operator_index
    );
    return {
      initial_index: break_point_index + 1,
      final_index: operator_index,
      value: left_operand,
    };
  } else {
    const left_operand = where_literals.slice(0, operator_index);
    return {
      initial_index: 0,
      final_index: operator_index,
      value: left_operand,
    };
  }
}

export function extract_comparision_operand_right(
  operator_index: number,
  where_literals: string[]
) {
  let break_point_index = null;

  for (let index = operator_index + 1; index < where_literals.length; index++) {
    // if we found "(" then try to find the counter part ")"
    // whole section from "("  to ")"  is part of the operand ( right)
    // set the index to position of the counter part and continue the loop
    const literal = where_literals[index];

    // check for the internal bracket
    if (literal === "(") {
      // found the initial bracket
      // try to find the counter part
      const index_of_counterpart = find_bracket_close_index(
        where_literals,
        "right",
        index + 1
      );
      if (index_of_counterpart > -1) {
        // found the counter part
        // continue the loop
        index = index_of_counterpart;
      } else {
        // not found the counter part
        throw new Error(
          'SQL syntax error | ")" can be found. You missed to put ")".'
        );
      }
    }

    if (
      literal === ")" ||
      literal.toUpperCase() === "AND" ||
      literal.toUpperCase() === "OR"
    ) {
      break_point_index = index;
      break;
    }
  }

  if (break_point_index !== null) {
    const right_operand = where_literals.slice(
      operator_index + 1,
      break_point_index
    );
    return {
      initial_index: operator_index + 1,
      final_index: break_point_index,
      value: right_operand,
    };
  } else {
    const right_operand = where_literals.slice(operator_index + 1);
    return {
      initial_index: operator_index + 1,
      final_index: where_literals.length,
      value: right_operand,
    };
  }
}

export function replace_comparision_operand_reference(
  operator: string,
  index: number,
  where_literals: string[],
  comparision_operands: comparision_operands_interface[]
) {
  const where_literals_copy: any = JSON.parse(JSON.stringify(where_literals));

  // equal operator
  const right_operand = extract_comparision_operand_right(
    index,
    where_literals_copy
  );
  const left_operand = extract_comparision_operand_left(
    index,
    where_literals_copy
  );

  //  manage the right one
  comparision_operands.push({ value: right_operand.value, operator: operator });
  // replece the where array with reference
  const right_index = comparision_operands.length - 1;
  where_literals_copy[index + 1] = `COPR_${right_index}`;
  where_literals_copy.forEach((p: any, i: any) => {
    if (i < right_operand.final_index && i > index + 1) {
      where_literals_copy[i] = null;
    }
  });

  // manage the left operand
  comparision_operands.push({ value: left_operand.value, operator: operator });
  // replace the where literal with reference
  const left_index = comparision_operands.length - 1;
  where_literals_copy[index - 1] = `COPL_${left_index}`;
  where_literals_copy.forEach((p: any, i: any) => {
    if (i < index - 1 && i >= left_operand.initial_index) {
      where_literals_copy[i] = null;
    }
  });

  return where_literals_copy.filter((p: any) => p !== null);
}

export function squeeze_comparision_operator_operand(
  where_literals: string[],
  comparision_operands: comparision_operands_interface[]
) {
  const comparision_operators = ["!=", "<", ">", "<=", ">=", "="];
  let number_of_comparision_operator_squeeze = 0;
  const number_of_comparision_operators_in_where_array: number =
    found_number_of_comparision_operators(where_literals);

  const squeeze = (
    where_literals_copy: string[],
    cb: (result: string[]) => void
  ) => {
    let number_of_operator_found = 0;

    for (let index = 0; index < where_literals_copy.length; index++) {
      const literal = where_literals_copy[index];

      if (comparision_operators.includes(literal)) {
        // yes comparision operator
        number_of_operator_found = number_of_operator_found + 1;

        if (number_of_operator_found > number_of_comparision_operator_squeeze) {
          where_literals_copy = replace_comparision_operand_reference(
            literal,
            index,
            where_literals_copy,
            comparision_operands
          );
          number_of_comparision_operator_squeeze =
            number_of_comparision_operator_squeeze + 1;
          break;
        } else {
          continue;
        }
      }
    }

    if (
      number_of_comparision_operator_squeeze !==
      number_of_comparision_operators_in_where_array
    ) {
      squeeze(where_literals_copy, cb);
    } else {
      cb(where_literals_copy);
    }
  };

  return new Promise<string[]>((resolve, reject) => {
    squeeze(JSON.parse(JSON.stringify(where_literals)), (result) =>
      resolve(result)
    );
  });
}

export function group_comparision_cell(
  where_literals: string[],
  comparision_cell: comparision_cell_interface[]
) {
  // recursive function
  const group = (
    where_literals_copy: string[],
    cb: result_callback_interface
  ) => {
    for (let index = 0; index < where_literals_copy.length; index++) {
      const literal: string = where_literals_copy[index];

      if (comparision_operators.includes(literal)) {
        // found the comparision operator
        const left_operand = where_literals_copy[index - 1];
        const right_operand = where_literals_copy[index + 1];
        comparision_cell.push({
          left_operand,
          right_operand,
          operator: literal as comparision_operator,
        });
        const last_index = comparision_cell.length - 1;
        where_literals_copy.splice(index - 1, 3, `C_${last_index}`);
        break;
      }
    }

    // check if comparision operators is still there
    if (
      where_literals_copy.some((val) => comparision_operators.includes(val))
    ) {
      group(where_literals_copy, cb);
    } else {
      cb(where_literals_copy);
    }
  };

  return new Promise<string[]>((resolve, reject) => {
    group(JSON.parse(JSON.stringify(where_literals)), (result) =>
      resolve(result)
    );
  });
}

export function group_bracket_cell(
  where_literals: string[],
  group_cell: group_cell_interface[]
) {
  // recursive function
  const group = (
    where_literals_copy: string[],
    cb: result_callback_interface
  ) => {
    let initial_index = null;
    let final_index = null;

    for (let i = 0; i < where_literals_copy.length; i++) {
      const val = where_literals_copy[i];
      if (val === "(") {
        initial_index = i;
      } else if (val === ")") {
        final_index = i;
        if (initial_index !== null && final_index !== null) {
          // found the new bracket group
          group_cell.push({
            value: where_literals_copy.slice(initial_index + 1, final_index),
          });
        }

        break;
      }
    }

    if (initial_index !== null && final_index !== null) {
      const index = group_cell.length - 1;
      where_literals_copy.splice(
        initial_index,
        final_index - initial_index + 1,
        `G_${index}`
      );
      group(where_literals_copy, cb);
    } else if (initial_index === null && final_index === null) {
      group_cell.push({ value: where_literals_copy.slice(0) });
      where_literals_copy.splice(0);
      cb(where_literals_copy);
    }
  };

  return new Promise<string[]>((resolve, reject) => {
    group(JSON.parse(JSON.stringify(where_literals)), (result) =>
      resolve(result)
    );
  });
}

// if the string contain number starting from first position then extract that number
// i.e 123Hasjkhka , 34234jsdkajdkla -> 123, 34234
// invalid - khkdsakh34343kjh
export function extract_number_from_string(str: string) {
  // chck if the str is pure number
  // if pure number then just return the pure number
  if (is_number(str)) {
    // pure number
    return new BigNumber(str);
  } else {
    // not a number
    const splited_value = str.split("");
    // will hold the number found in the string
    const number_found: string[] = [];

    for (let index = 0; index < splited_value.length; index++) {
      const value = splited_value[index];
      if (is_number(value)) {
        // number pure
        number_found.push(value);
        continue;
      } else {
        // string found
        break;
      }
    }

    if (number_found.length !== 0) {
      // there was number in the string
      return new BigNumber(number_found.join(""));
    } else {
      // str is pure string
      return new BigNumber(0);
    }
  }
}

export function evaluate_comparision_operand(operand: string[], data: any) {
  // is there arithmatic operator or not
  const found_arithmatic_operator = operand.some((val) =>
    arithmatic_operators_with_modu.includes(val)
  );
  const extended_arithmatic_operators = [
    ...arithmatic_operators_with_modu,
    "(",
    ")",
  ];

  let final_data = operand.map((p) => {
    if (!extended_arithmatic_operators.includes(p)) {
      // not a arithmatic operators
      if (is_number(p)) {
        // is pure number
        return new BigNumber(p);
      } else if (is_pure_sql_string(p)) {
        // pure sql string
        // try to find weather it contain number sequence at first position onward
        if (found_arithmatic_operator) {
          // need to convert the pure string to 0 as per SQL standard
          return extract_number_from_string(p.slice(1, p.length - 1).trim());
        } else {
          // no need to convert the pure sql string to 0
          // because there is no arithmatic operators in comparision operand
          // just remove the single quote from the pure string and return
          return p.slice(1, p.length - 1);
        }
      } else {
        // TODO: if there is no such column in the data return or throw error
        const column_value = data[p];
        if (is_number(column_value)) {
          // is pure number
          return new BigNumber(column_value);
        } else if (is_pure_sql_string(column_value)) {
          // pure sql string
          if (found_arithmatic_operator) {
            // need to convert the pure string to 0 as per SQL standard
            return extract_number_from_string(
              column_value.slice(1, column_value.length - 1).trim()
            );
          } else {
            // no need to convert the pure sql string to 0
            // because there is no arithmatic operators in comparision operand
            // just remove the single quote from the pure string and return
            return column_value.slice(1, column_value.length - 1).trim();
          }
        }
      }
    } else {
      // arithmatic operator
      return p;
    }
  });

  final_data = manage_of(final_data);
  const evaluate = async (
    arithmatic_literals: any[],
    cb: (result: BigNumber) => void
  ) => {
    let initial_index = null;
    let final_index = null;

    for (let index = 0; index < arithmatic_literals.length; index++) {
      const value = arithmatic_literals[index];

      if (value === "(") {
        initial_index = index;
      } else if (value === ")") {
        final_index = index;
        break;
      }
    }

    // TODO:  throw the error if there is initial index but no final index

    if (initial_index !== null && final_index !== null) {
      // found the bracket
      const actual_arithmatic = arithmatic_literals.slice(
        initial_index + 1,
        final_index
      );
      const result = await do_arithmatics(actual_arithmatic);
      arithmatic_literals.splice(
        initial_index,
        final_index - initial_index + 1,
        result
      );
      evaluate(arithmatic_literals, cb);
    } else {
      // no brackets found
      const actual_arithmatic = arithmatic_literals.slice(0);
      const result = await do_arithmatics(actual_arithmatic);
      // replace arithmatic_literals
      arithmatic_literals.splice(0, arithmatic_literals.length, result);
      cb(arithmatic_literals[0]);
    }
  };

  return new Promise<BigNumber>((resolve, reject) => {
    evaluate(final_data, (result) => resolve(result));
  });
}

//  evaluate the comparision operands

export async function evaluate_comparision_operands(
  comparision_operands: comparision_operands_interface[],
  data: any
) {
  for (let index = 0; index < comparision_operands.length; index++) {
    const operand: string[] = comparision_operands[index].value;
    comparision_operands[index]["result"] = await evaluate_comparision_operand(
      operand,
      data
    );
  }

  return comparision_operands;
}

export function evaluate_comparision_statement(
  comparision_cell: comparision_cell_interface[],
  comparision_operands: comparision_operands_interface[]
) {
  for (let index = 0; index < comparision_cell.length; index++) {
    const left_operand: any = comparision_cell[index].left_operand;
    const right_operand: any = comparision_cell[index].right_operand;
    const operator = comparision_cell[index].operator;

    const left_operand_value =
      comparision_operands[+left_operand.split("_")[1]].result;
    const right_operand_value =
      comparision_operands[+right_operand.split("_")[1]].result;

    comparision_cell[index].result = resolve_compare(
      left_operand_value,
      right_operand_value,
      operator
    );
  }
}

export function resolve_AND(group_cell_literals: any[]) {
  function evaluate_AND(results: any[], cb: (result: any[]) => void) {
    for (let index = 0; index < results.length; index++) {
      const literal = results[index];
      if (literal === logical_operators[0]) {
        const left_value = results[index - 1];
        const right_value = results[index + 1];
        results.splice(index - 1, 3, left_value && right_value);
        break;
      }
    }

    if (results.some((val) => logical_operators[0] === val)) {
      // there is more AND operator
      // call this function again
      evaluate_AND(results, cb);
    } else {
      // there is no AND operator
      // call the callback function
      cb(results);
    }
  }

  return new Promise<any[]>((resolve, reject) => {
    evaluate_AND(group_cell_literals, (data) => resolve(data));
  });
}

export function resolve_OR(group_cell_literals: any[]) {
  function evaluate_OR(results: any[], cb: (result: any[]) => void) {
    for (let index = 0; index < results.length; index++) {
      const literal = results[index];
      if (literal === logical_operators[1]) {
        // OR logical operator
        const left_value = results[index - 1];
        const right_value = results[index + 1];
        results.splice(index - 1, 3, left_value || right_value);
        break;
      }
    }

    if (results.some((val) => logical_operators[1] === val)) {
      // there is more OR operator
      // call this function again
      evaluate_OR(results, cb);
    } else {
      // there is no AND operator
      // call the callback function
      cb(results);
    }
  }

  return new Promise<any[]>((resolve, reject) => {
    evaluate_OR(group_cell_literals, (data) => resolve(data));
  });
}

export async function evaluate_group_cell(
  group_cell: group_cell_interface[],
  comparision_cell: comparision_cell_interface[]
) {
  for (let index = 0; index < group_cell.length; index++) {
    let results: any[] = [];
    const g_cell: any = group_cell[index];

    for (const gv of g_cell.value) {
      if (!logical_operators.includes(gv)) {
        // not a lgical operator
        const [type, index] = gv.split("_");
        if (type === "C") {
          results.push(comparision_cell[+index].result);
        } else if (type === "G") {
          results.push(group_cell[+index].result);
        }
      } else {
        // logical operator
        results.push(gv);
      }
    }

    // manage &&

    results = await resolve_AND(results);
    // manage the or operators
    results = await resolve_OR(results);

    g_cell.result = results[0];
  }

  return group_cell[group_cell.length - 1].result;
}

export async function single_table_where_clause(
  data: any,
  where_statement: string
) {
  let where_literals = make_where_literal(where_statement);
  let comparision_operands: comparision_operands_interface[] = [];
  where_literals = await squeeze_comparision_operator_operand(
    where_literals,
    comparision_operands
  );
  const comparision_cell: comparision_cell_interface[] = [];
  where_literals = await group_comparision_cell(
    where_literals,
    comparision_cell
  );
  const group_cell: group_cell_interface[] = [];
  where_literals = await group_bracket_cell(where_literals, group_cell);
  await evaluate_comparision_operands(comparision_operands, data);
  evaluate_comparision_statement(comparision_cell, comparision_operands);
  const can_pass = await evaluate_group_cell(group_cell, comparision_cell);

  if (can_pass) {
    return data;
  } else {
    return null;
  }
}
