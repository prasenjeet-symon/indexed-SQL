import BigNumber from "bignumber.js";
import {
  comparision_cell_interface,
  comparision_operands_interface,
  group_cell_interface,
} from "../lib/main.interface";
import {
  combine_single_quoted_value,
  evaluate_comparision_operand,
  evaluate_comparision_operands,
  extract_comparision_operand_left,
  extract_comparision_operand_right,
  group_bracket_cell,
  group_comparision_cell,
  make_where_literal,
  replace_comparision_operand_reference,
  resolve_AND,
  squeeze_comparision_operator_operand,
} from "../lib/where_clause";
/**
 * @jest-environment jsdom
 */
test("where_clause/make_where_literal", () => {
  const test_data: string = `     name='Harry' ANDage= 23`;
  const result = make_where_literal(test_data);
  expect(result).toEqual(["name", "=", "'Harry'", "AND", "age", "=", "23"]);

  const test_data_1: string = `   (  name='Harry' ANDage= 23) OR gender = 'Male' `;
  const result_1 = make_where_literal(test_data_1);
  expect(result_1).toEqual([
    "(",
    "name",
    "=",
    "'Harry'",
    "AND",
    "age",
    "=",
    "23",
    ")",
    "OR",
    "gender",
    "=",
    "'Male'",
  ]);
});

test("where_clause/combine_single_quoted_value", async () => {
  const test_data = ["name", "=", "'", "Harry", "'", "AND", "age", "=", "23"];
  const result = await combine_single_quoted_value(test_data);
  expect(result).toEqual(["name", "=", "'Harry'", "AND", "age", "=", "23"]);
});

test("where_clause/extract_comparision_operand_left", () => {
  const test_data: string[] = make_where_literal(
    ` ( age - 12 ) * 12 = 12 AND name = 'Harry'`
  );
  const result = extract_comparision_operand_left(7, test_data);
  expect(result.final_index).toBe(7);
  expect(result.initial_index).toBe(0);
  expect(result.value).toEqual(make_where_literal("( age - 12 ) * 12"));

  const test_data_1: string[] = make_where_literal(
    ` gender = 'Male' OR ( age - 12 ) * 12 = 12 AND name = 'Harry'`
  );
  const result_1 = extract_comparision_operand_left(11, test_data_1);
  expect(result_1.final_index).toBe(11);
  expect(result_1.initial_index).toBe(4);
  expect(result_1.value).toEqual(make_where_literal("( age - 12 ) * 12"));
});

test("where_clause/extract_comparision_operand_right", () => {
  const test_data: string[] = make_where_literal(
    ` 9 = ( age - ( 12 ) ) * 12 AND name = 'Harry'`
  );
  const result = extract_comparision_operand_right(1, test_data);
  expect(result.final_index).toBe(11);
  expect(result.initial_index).toBe(2);
  expect(result.value).toEqual(make_where_literal("( age - ( 12 ) ) * 12"));

  const test_data_1: string[] = make_where_literal(
    ` 9 = ( age - 12 ) * 12 AND name = 'Harry'`
  );
  const result_1 = extract_comparision_operand_right(11, test_data_1);
  expect(result_1.final_index).toBe(13);
  expect(result_1.initial_index).toBe(12);
  expect(result_1.value).toEqual(make_where_literal("'Harry'"));
});

test("where_clause/replace_comparision_operand_reference", () => {
  const comparision_operands: {
    value: string[];
    operator: string;
    result?: any;
  }[] = [];

  const test_data: string[] = make_where_literal(
    ` 9 = ( age - 12 ) * 12 AND name = 'Harry'`
  );
  const result = replace_comparision_operand_reference(
    "=",
    1,
    test_data,
    comparision_operands
  );
  expect(result).toEqual(
    make_where_literal(` COPL_1 = COPR_0 AND name = 'Harry'`)
  );
  comparision_operands.forEach((p, i) => {
    if (i === 0) {
      expect(p.value).toEqual(make_where_literal(`( age - 12 ) * 12`));
    } else {
      expect(p.value).toEqual(make_where_literal(`9`));
    }
  });
});

test("where_clause/squeeze_comparision_operator_operand", async () => {
  const comparision_operands: comparision_operands_interface[] = [];

  const test_data: string[] = make_where_literal(
    ` ( 9 ) = ( age - 12 ) * 12 AND name = 'Harry'`
  );

  const result = await squeeze_comparision_operator_operand(
    test_data,
    comparision_operands
  );
  expect(result).toEqual(
    make_where_literal(` COPL_1 = COPR_0 AND COPL_3 = COPR_2`)
  );

  comparision_operands.forEach((p, i) => {
    switch (i) {
      case 0:
        expect(p.value).toEqual(make_where_literal("( age - 12 ) * 12"));
        break;

      case 1:
        expect(p.value).toEqual(make_where_literal("( 9 )"));
        break;

      case 2:
        expect(p.value).toEqual(make_where_literal("'Harry'"));
        break;

      case 3:
        expect(p.value).toEqual(make_where_literal("name"));
        break;
    }
  });
});

test("where_clause/group_comparision_cell", async () => {
  const group_cell: group_cell_interface[] = [];
  const comparision_cell: comparision_cell_interface[] = [];
  const comparision_operands: comparision_operands_interface[] = [];

  const test_data: string[] = await squeeze_comparision_operator_operand(
    make_where_literal(`( 9 ) = ( age - 12 ) * 12 AND name = 'Harry'`),
    comparision_operands
  );
  const result = await group_comparision_cell(test_data, comparision_cell);

  expect(result).toEqual(make_where_literal(`C_0 AND C_1`));

  comparision_cell.forEach((p, i) => {
    switch (i) {
      case 0:
        expect(p).toEqual({
          right_operand: `COPR_0`,
          left_operand: `COPL_1`,
          operator: `=`,
          result: undefined,
        });
        break;

      case 1:
        expect(p).toEqual({
          right_operand: `COPR_2`,
          left_operand: `COPL_3`,
          operator: `=`,
          result: undefined,
        });
        break;
    }
  });

  // testing the group bracket
  const result_bracket = await group_bracket_cell(result, group_cell);
  expect(result_bracket).toEqual([]);
});

test("where_clause/evaluate_comparision_operand", async () => {
  const test_data: string[] = make_where_literal(` 2 + 4 - 12 + age `);
  const data = { age: "12" };
  const result = await evaluate_comparision_operand(test_data, data);
  expect(result.toString()).toBe("6");

  const test_data_1: string[] = make_where_literal(` 2 + 4 - 12 + age * name `);
  const data_1 = { age: "12", name: "'Harry'" };
  const result_1 = await evaluate_comparision_operand(test_data_1, data_1);
  expect(result_1.toString()).toBe("-6");

  const test_data_2: string[] = make_where_literal(
    ` 2 +  4 - ( 12  + age ) * name `
  );
  const data_2 = { age: "12", name: "'Harry'" };
  const result_2 = await evaluate_comparision_operand(test_data_2, data_2);
  expect(result_2.toString()).toBe("6");

  const test_data_3: string[] = make_where_literal(
    ` 2 +  4 - ( 12  + age ) * name `
  );
  const data_3 = { age: "12", name: "'10Harry'" };
  const result_3 = await evaluate_comparision_operand(test_data_3, data_3);
  expect(result_3.toString()).toBe("-234");
});

test("where_clause/evaluate_comparision_operands", async () => {
  const comparision_operands: comparision_operands_interface[] = [
    {
      value: make_where_literal(` 2 + 4 - 12 + age `),
      operator: "=",
      result: null,
    },
    {
      value: make_where_literal(` 12 - 4 + 12 - age `),
      operator: "=",
      result: null,
    },
  ];

  const data = { age: "12" };
  await evaluate_comparision_operands(comparision_operands, data);

  comparision_operands.forEach((p, i) => {
    switch (i) {
      case 0:
        expect(p.result.toString()).toBe("6");
        break;

      case 1:
        expect((p.result as BigNumber).toString()).toBe("8");
        break;
    }
  });
});

test("where_clause/resolve_AND", async () => {
  const test_data = make_where_literal(
    ` true OR false AND  true OR false  `
  ).map((p) => {
    if (p === "true") {
      return true;
    } else if (p === "false") {
      return false;
    } else {
      return p;
    }
  });

  const result = await resolve_AND(test_data);
  expect(result.join(" ")).toBe(`true OR false OR false`);
});
