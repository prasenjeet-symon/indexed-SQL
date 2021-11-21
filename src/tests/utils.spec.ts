import BigNumber from "bignumber.js";
import {
  combine_all_sql_spaced_literals,
  make_select_literals,
} from "../lib/single-table/select_clause";
import {
  compare_dates,
  compare_numbers,
  compare_strings,
  compare_string_and_number,
  convert_to_number,
  detect_query_type,
  do_arithmatics,
  extract_number_from_string,
  filter_unique_object_in_array,
  findIndexDir,
  find_bracket_close_index,
  generalTrim,
  group_by,
  is_comparing_dates,
  is_comparing_numbers,
  is_comparing_string,
  is_comparing_string_and_number,
  is_number,
  is_pure_sql_string,
  is_string_date,
  is_time_string,
  is_two_obejct_equal,
  is_two_obejct_type_equal,
  is_year_string,
  manage_of,
  parse_order_by,
  remove_char_form_end,
  resolve_compare,
  sort_array_with_obj,
  sort_by,
  sort_similar_obj_in_array,
  union_maker,
  unite_sources,
} from "../lib/utils";

/**
 * @jest-environment jsdom
 */

test("utils/findIndexDir", () => {
  const test_data = ["Harry", "Marry", "Jerry", "(", ")", "Omm"];

  const result_1 = findIndexDir(
    test_data,
    "Marry",
    "left",
    test_data.length - 1
  );
  const result_2 = findIndexDir(
    test_data,
    "Harry",
    "left",
    test_data.length - 1
  );

  const result_3 = findIndexDir(test_data, "(", "right", 2);
  const result_4 = findIndexDir(test_data, "Omm", "right", 0);

  expect(result_1).toBe(1);
  expect(result_2).toBe(0);

  expect(result_3).toBe(3);
  expect(result_4).toBe(5);
});

test("utils/manage_of", () => {
  // test with first data
  const test_data_1: string[] = ` 6 + 7 `.trim().replace(/\s+/, " ").split(" ");
  const result_1 = manage_of(test_data_1);
  expect(result_1).toEqual(["6", "+", "7"]);

  const test_data_2: string[] = ` ( 6 + 7 ) `
    .trim()
    .replace(/\s+/, " ")
    .split(" ");
  const result_2 = manage_of(test_data_2);
  expect(result_2).toEqual(["(", "6", "+", "7", ")"]);

  const test_data_3: string[] = `4 ( 6 + 7 ) 4`
    .trim()
    .replace(/\s+/, " ")
    .split(" ");
  const result_3 = manage_of(test_data_3);
  expect(result_3).toEqual(["4", "*", "(", "6", "+", "7", ")", "*", "4"]);

  const test_data_4: string[] = `4 ( 6 + 7 ) + 4`
    .trim()
    .replace(/\s+/, " ")
    .split(" ");
  const result_4 = manage_of(test_data_4);
  expect(result_4).toEqual(["4", "*", "(", "6", "+", "7", ")", "+", "4"]);

  const test_data_5: string[] = `( 4 ( 6 + 7 ) + 4 )`
    .trim()
    .replace(/\s+/, " ")
    .split(" ");
  const result_5 = manage_of(test_data_5);
  expect(result_5).toEqual([
    "(",
    "4",
    "*",
    "(",
    "6",
    "+",
    "7",
    ")",
    "+",
    "4",
    ")",
  ]);
});

test("utils/do_arithmatics", async () => {
  const test_data = ` 2 + 4 `
    .trim()
    .replace(/\s+/, " ")
    .split(" ")
    .map((val) => (is_number(val) ? new BigNumber(val) : val));
  const result = await do_arithmatics(test_data);
  expect(result.toString()).toBe("6");

  const test_data_1 = ` 4 / 2 * 2 + 4 - 1 `
    .trim()
    .replace(/\s+/, " ")
    .split(" ")
    .map((val) => (is_number(val) ? new BigNumber(val) : val));
  const result_1 = await do_arithmatics(test_data_1);
  expect(result_1.toString()).toBe("7");

  const test_data_2 = ` 2 + 4 - 5 + 9 * 0 `
    .trim()
    .replace(/\s+/, " ")
    .split(" ")
    .map((val) => (is_number(val) ? new BigNumber(val) : val));
  const result_2 = await do_arithmatics(test_data_2);
  expect(result_2.toString()).toBe("1");
});

test("utils/is_pure_sql_string", () => {
  const test_data = `'Harry'`;
  const result = is_pure_sql_string(test_data);
  expect(result).toBeTruthy();

  const test_data_1 = `Harry`;
  const result_1 = is_pure_sql_string(test_data_1);
  expect(result_1).toBeFalsy();
});

test("utils/is_number", () => {
  const test_data = "23";
  const result = is_number(test_data);
  expect(result).toBeTruthy();

  const test_data_1 = "Joi";
  const result_1 = is_number(test_data_1);
  expect(result_1).toBeFalsy();

  const test_data_2 = "23Joi";
  const result_2 = is_number(test_data_2);
  expect(result_2).toBeFalsy();
});

test("utils/extract_number_from_string", () => {
  const test_data: string = `123Harry`;
  const result = extract_number_from_string(test_data);
  expect(result.toString()).toBe("123");

  const test_data_1: string = `1003Harry`;
  const result_1 = extract_number_from_string(test_data_1);
  expect(result_1.toString()).toBe("1003");
});

test("utils/is_string_date", () => {
  const test_data = `2020-03-12 12:23:02`; // sql datetime
  const result = is_string_date(test_data);
  expect(result).toBeTruthy();

  const test_data_1 = `2020-AL-12 12:23:02`; // sql datetime
  const result_1 = is_string_date(test_data_1);
  expect(result_1).toBeFalsy();

  const test_data_2 = `2020-03-12`; // sql datetime
  const result_2 = is_string_date(test_data_2);
  expect(result_2).toBeTruthy();
});

test("utils/is_comparing_dates", () => {
  const result = is_comparing_dates(
    "2020-12-02 09:20:22",
    "1999-03-23 10:23:22",
    "="
  );
  expect(result).toBeTruthy();

  const result_1 = is_comparing_dates("2020-12-02 09:20:22", "Harry2020", "=");
  expect(result_1).toBeFalsy();
});

test("utils/compare_dates", () => {
  const left_date = `2020-02-12`;
  const right_date = `2020-03-12`;

  const result = compare_dates(left_date, right_date, "=");
  expect(result).toBeFalsy();

  const result_1 = compare_dates(left_date, right_date, ">");
  expect(result_1).toBeFalsy();

  const result_3 = compare_dates(left_date, right_date, "<");
  expect(result_3).toBeTruthy();
});

test("utils/is_comparing_string", () => {
  const result = is_comparing_string("Harry", "Marry", "=");
  expect(result).toBeTruthy();

  const result_1 = is_comparing_string("Harry", 34, "!=");
  expect(result_1).toBeFalsy();

  const result_2 = is_comparing_string("34", "45", "!=");
  expect(result_2).toBeTruthy();
});

test("utils/compare_strings", () => {
  const result = compare_strings("Harry", "Marry", ">");
  expect(result).toBeFalsy();

  const result_1 = compare_strings("Harry", "Marry", "<");
  expect(result_1).toBeTruthy();
});

test("utils/is_comparing_numbers", () => {
  const result = is_comparing_numbers(
    new BigNumber("12"),
    new BigNumber("23"),
    "!="
  );
  expect(result).toBeTruthy();

  const result_1 = is_comparing_numbers("12", new BigNumber("23"), "!=");
  expect(result_1).toBeFalsy();
});

test("utils/compare_numbers", () => {
  const result = compare_numbers(new BigNumber("12"), new BigNumber("23"), "=");
  expect(result).toBeFalsy();

  const result_1 = compare_numbers(
    new BigNumber("12"),
    new BigNumber("23"),
    ">"
  );
  expect(result_1).toBeFalsy();

  const result_2 = compare_numbers(
    new BigNumber("12"),
    new BigNumber("23"),
    "<"
  );
  expect(result_2).toBeTruthy();
});

test("utils/is_comparing_string_and_number", () => {
  const result = is_comparing_string_and_number(new BigNumber(12), "12", "!=");
  expect(result).toBeTruthy();

  const result_1 = is_comparing_string_and_number(
    new BigNumber(12),
    new BigNumber(12),
    "<"
  );
  expect(result_1).toBeFalsy();

  const result_2 = is_comparing_string_and_number(
    "12",
    new BigNumber(1121),
    "<"
  );
  expect(result_2).toBeTruthy();
});

test("utils/compare_string_and_number", () => {
  const result = compare_string_and_number(12, "Harry", "=");
  expect(result).toBeFalsy();

  const result_1 = compare_string_and_number(12, "Marry", "<");
  expect(result_1).toBeTruthy();

  const result_2 = compare_string_and_number("11", 13, ">");
  expect(result_2).toBeTruthy();
});

test("utils/resolve_compare", () => {
  const result = resolve_compare(new BigNumber(12), new BigNumber(13), "<");
  expect(result).toBe(true);

  const result_1 = resolve_compare("12", new BigNumber(13), "<");
  expect(result_1).toBe(false);

  const result_2 = resolve_compare("2020-02-12", "2020-01-12", ">");
  expect(result_2).toBe(true);
});

test("utils/find_bracket_close_index", () => {
  const test_data = [
    "(",
    "(",
    "5",
    "-",
    "3",
    ")",
    "+",
    "(",
    "4",
    "+",
    "3",
    ")",
    ")",
  ];
  const result = find_bracket_close_index(test_data, "right", 1);
  expect(result).toBe(12);
});

test("utils/remove_char_form_end", async () => {
  const remove_char_form_end_promise = (
    start_char: string,
    end_char: string,
    arr: string[]
  ) => {
    return new Promise<string[]>((resolve, reject) => {
      remove_char_form_end(start_char, end_char, arr, (result) =>
        resolve(result)
      );
    });
  };

  const test_data = `[[[ name, fame , game game ]]]`.split("");
  const result = await remove_char_form_end_promise("[", "]", test_data);
  expect(result).toEqual(` name, fame , game game `.split(""));
});

test("utils/generalTrim", async () => {
  const tets_data = `(( My name [Heu] is prasenjeet))`;
  const result = await generalTrim(tets_data, "(");
  expect(result).toBe(`My name [Heu] is prasenjeet`);

  const test_data_1 = `[[ [My name] [Heu] is [prasenjeet]]]`;
  const result_1 = await generalTrim(test_data_1, "[");
  expect(result_1).toBe(`[My name] [Heu] is [prasenjeet]`);
});

test("utils/is_two_obejct_equal", () => {
  const test_data_1 = { name: "Harry", age: 12, gender: "Male" };
  const test_data_2 = { name: "Harry", age: 12, gender: "Male" };

  const result = is_two_obejct_equal(test_data_1, test_data_2);
  expect(result).toBe(true);

  const test_data_3 = { name: "Harry", age: 12, gender: "Male" };
  const test_data_4 = { name: "Harry", age: "12", gender: "Male" };

  const result_1 = is_two_obejct_equal(test_data_3, test_data_4);
  expect(result_1).toBe(false);

  const test_data_5 = { name: "Harry", age: 12, gender: "Male" };
  const test_data_6 = { name: "Harry", gender: "Male" };

  const result_2 = is_two_obejct_equal(test_data_5, test_data_6);
  expect(result_2).toBe(false);
});

test("utils/filter_unique_object_in_array", async () => {
  const test_data = [
    { name: "Harry", age: 12, gender: "Other" },
    { name: "Marry", age: 13, gender: "Female" },
    { name: "Karry", age: 12, gender: "Female" },
    { name: "Harry", age: 12, gender: "Other" },
    { name: "Harry", age: 12, gender: "Other" },
    { name: "Harry", age: 12, gender: "Other" },
  ];

  const result = await filter_unique_object_in_array(test_data);
  expect(result).toEqual([
    { name: "Harry", age: 12, gender: "Other" },
    { name: "Marry", age: 13, gender: "Female" },
    { name: "Karry", age: 12, gender: "Female" },
  ]);
});

test("utils/is_two_obejct_type_equal", () => {
  const test_data = [
    { name: "Harry", age: 12, gender: "Male" },
    { name: "Marry", age: 34, gender: "Female" },
  ];

  const result = is_two_obejct_type_equal(test_data[0], test_data[1]);
  expect(result).toBe(true);

  // test 2
  const test_data_1 = [
    { name: "Harry", age: 12, gender: "Male" },
    { name: "Marry", age: "34", gender: "Female" },
  ];

  const result_1 = is_two_obejct_type_equal(test_data_1[0], test_data_1[1]);
  expect(result_1).toBe(false);
});

test("utils/union_maker", async () => {
  const test_data = [
    [
      { name: "Harry", age: 12, gender: "male" },
      { s_name: "Karry", s_age: 13, s_gender: "other" },
    ],
    [
      { f_name: "Harry", age: 12, g_gender: "male" },
      { k_name: "Jerry", age: 15, m_gender: "male" },
    ],
  ];

  const result = await union_maker(test_data[0], test_data[1], "union");
  expect(result).toEqual([
    { name: "Harry", age: 12, gender: "male" },
    { name: "Karry", age: 13, gender: "other" },
    { name: "Jerry", age: 15, gender: "male" },
  ]);

  // test 2
  const test_data_1 = [
    [
      { name: "Harry", age: 12, gender: "male" },
      { name: "Karry", age: 13, gender: "other" },
    ],
    [
      { name: "Harry", age: 12, gender: "male" },
      { name: "Jerry", age: 15, gender: "male" },
    ],
  ];

  const result_1 = await union_maker(
    test_data_1[0],
    test_data_1[1],
    "union_all"
  );
  expect(result_1).toEqual([
    { name: "Harry", age: 12, gender: "male" },
    { name: "Karry", age: 13, gender: "other" },
    { name: "Harry", age: 12, gender: "male" },
    { name: "Jerry", age: 15, gender: "male" },
  ]);
});

test("utils/group_by", async () => {
  const test_data = [
    { name: "Harry", age: 12, gender: "male" },
    { name: "Karry", age: 13, gender: "other" },
    { name: "Harry", age: 12, gender: "male" },
    { name: "Jerry", age: 15, gender: "male" },
  ];

  const result = await group_by(test_data, "age");
  expect(result).toEqual([
    { name: "Harry", age: 12, gender: "male" },
    { name: "Karry", age: 13, gender: "other" },
    { name: "Jerry", age: 15, gender: "male" },
  ]);
});

test("utils/sort_array_with_obj", () => {
  const test_data = [
    { name: "Harry", age: 12, gender: "male", birth_date: "2006-02-12" },
    { name: "Marry", age: 13, gender: "female", birth_date: "2009-12-12" },
    { name: "Divya", age: 10, gender: "female", birth_date: "2002-11-12" },
    { name: "Hena", age: 9, gender: "female", birth_date: "2001-03-12" },
    { name: "Rita", age: 8, gender: "female", birth_date: "2003-06-12" },
  ];

  const result = sort_array_with_obj(test_data, "age", "ASC");
  expect(result).toEqual([
    { name: "Rita", age: 8, gender: "female", birth_date: "2003-06-12" },
    { name: "Hena", age: 9, gender: "female", birth_date: "2001-03-12" },
    { name: "Divya", age: 10, gender: "female", birth_date: "2002-11-12" },
    { name: "Harry", age: 12, gender: "male", birth_date: "2006-02-12" },
    { name: "Marry", age: 13, gender: "female", birth_date: "2009-12-12" },
  ]);

  // test 2

  const test_data_1 = [
    { name: "Harry", age: 12, gender: "male", birth_date: "2006-02-12" },
    { name: "Marry", age: 13, gender: "female", birth_date: "2009-12-12" },
    { name: "Divya", age: 10, gender: "female", birth_date: "2002-11-12" },
    { name: "Hena", age: 9, gender: "female", birth_date: "2001-03-12" },
    { name: "Rita", age: 8, gender: "female", birth_date: "2003-06-12" },
  ];

  const result_1 = sort_array_with_obj(test_data_1, "birth_date", "DESC");
  expect(result_1).toEqual([
    { name: "Marry", age: 13, gender: "female", birth_date: "2009-12-12" },
    { name: "Harry", age: 12, gender: "male", birth_date: "2006-02-12" },
    { name: "Rita", age: 8, gender: "female", birth_date: "2003-06-12" },
    { name: "Divya", age: 10, gender: "female", birth_date: "2002-11-12" },
    { name: "Hena", age: 9, gender: "female", birth_date: "2001-03-12" },
  ]);
});

test("utils/sort_similar_obj_in_array", () => {
  const test_data = [
    { name: "Harry", age: 12, gender: "male", birth_date: "2006-02-12" },
    { name: "Marry", age: 13, gender: "female", birth_date: "2009-12-12" },
    { name: "Divya", age: 10, gender: "female", birth_date: "2002-11-12" },
    { name: "Hena", age: 9, gender: "female", birth_date: "2001-03-12" },
    { name: "Rita", age: 8, gender: "female", birth_date: "2018-06-12" },
    { name: "Gita", age: 8, gender: "female", birth_date: "2003-06-12" },
    { name: "Sita", age: 8, gender: "female", birth_date: "2012-06-12" },
    { name: "Mita", age: 8, gender: "female", birth_date: "2014-06-12" },
    { name: "Chita", age: 14, gender: "female", birth_date: "2003-06-12" },
    { name: "Aeeta", age: 16, gender: "female", birth_date: "2003-06-12" },
  ];

  const result = sort_similar_obj_in_array(
    sort_array_with_obj(test_data, "age", "ASC"),
    "age",
    "birth_date",
    "ASC"
  );
  expect(result).toEqual([
    { name: "Gita", age: 8, gender: "female", birth_date: "2003-06-12" },
    { name: "Sita", age: 8, gender: "female", birth_date: "2012-06-12" },
    { name: "Mita", age: 8, gender: "female", birth_date: "2014-06-12" },
    { name: "Rita", age: 8, gender: "female", birth_date: "2018-06-12" },
    { name: "Hena", age: 9, gender: "female", birth_date: "2001-03-12" },
    { name: "Divya", age: 10, gender: "female", birth_date: "2002-11-12" },
    { name: "Harry", age: 12, gender: "male", birth_date: "2006-02-12" },
    { name: "Marry", age: 13, gender: "female", birth_date: "2009-12-12" },
    { name: "Chita", age: 14, gender: "female", birth_date: "2003-06-12" },
    { name: "Aeeta", age: 16, gender: "female", birth_date: "2003-06-12" },
  ]);
});

test("utils/sort_by", () => {
  const test_data = [
    { name: "Harry", age: 12, gender: "male", birth_date: "2006-02-12" },
    { name: "Marry", age: 13, gender: "female", birth_date: "2009-12-12" },
    { name: "Divya", age: 10, gender: "female", birth_date: "2002-11-12" },
    { name: "Hena", age: 9, gender: "female", birth_date: "2001-03-12" },
    { name: "Rita", age: 8, gender: "female", birth_date: "2018-06-12" },
    { name: "Gita", age: 8, gender: "female", birth_date: "2003-06-12" },
    { name: "Sita", age: 8, gender: "female", birth_date: "2012-06-12" },
    { name: "Mita", age: 8, gender: "female", birth_date: "2014-06-12" },
    { name: "Chita", age: 14, gender: "female", birth_date: "2003-06-12" },
    { name: "Aeeta", age: 16, gender: "female", birth_date: "2003-06-12" },
  ];

  const result = sort_by(test_data, [
    { key_name: "age", type: "ASC" },
    { key_name: "birth_date", type: "ASC" },
  ]);
  expect(result).toEqual([
    { name: "Gita", age: 8, gender: "female", birth_date: "2003-06-12" },
    { name: "Sita", age: 8, gender: "female", birth_date: "2012-06-12" },
    { name: "Mita", age: 8, gender: "female", birth_date: "2014-06-12" },
    { name: "Rita", age: 8, gender: "female", birth_date: "2018-06-12" },
    { name: "Hena", age: 9, gender: "female", birth_date: "2001-03-12" },
    { name: "Divya", age: 10, gender: "female", birth_date: "2002-11-12" },
    { name: "Harry", age: 12, gender: "male", birth_date: "2006-02-12" },
    { name: "Marry", age: 13, gender: "female", birth_date: "2009-12-12" },
    { name: "Chita", age: 14, gender: "female", birth_date: "2003-06-12" },
    { name: "Aeeta", age: 16, gender: "female", birth_date: "2003-06-12" },
  ]);
});

test("utils/unite_sources", async () => {
  const test_data = [
    [
      { name: "Harry", age: 12, gender: "male" },
      { name: "Karry", age: 13, gender: "other" },
    ],
    [
      { name: "Harry", age: 12, gender: "male" },
      { name: "Jerry", age: 15, gender: "male" },
    ],
  ];

  const union_types = [{ type: "union", data: "UNION ALL" }];
  const result = await unite_sources(test_data, union_types);
  expect(result).toEqual([
    { name: "Harry", age: 12, gender: "male" },
    { name: "Karry", age: 13, gender: "other" },
    { name: "Harry", age: 12, gender: "male" },
    { name: "Jerry", age: 15, gender: "male" },
  ]);

  // test 2
  const test_data_1 = [
    [
      { name: "Harry", age: 12, gender: "male" },
      { name: "Karry", age: 13, gender: "other" },
    ],
    [
      { name: "Harry", age: 12, gender: "male" },
      { name: "Jerry", age: 15, gender: "male" },
    ],
  ];

  const union_types_1 = [{ type: "union", data: "UNION" }];
  const result_1 = await unite_sources(test_data_1, union_types_1);
  expect(result_1).toEqual([
    { name: "Harry", age: 12, gender: "male" },
    { name: "Karry", age: 13, gender: "other" },
    { name: "Jerry", age: 15, gender: "male" },
  ]);
});

test("utils/parse_order_by", () => {
  const test_data = combine_all_sql_spaced_literals(
    make_select_literals(` ORDER BY name ASC, gender DESC`)
  );
  const result = parse_order_by(test_data);
  expect(result).toEqual([
    { key_name: "name", type: "ASC" },
    { key_name: "gender", type: "DESC" },
  ]);
});

describe("utils/convert_to_number", () => {
  test("test_1", () => {
    const test_data = "23.45543";
    const result = convert_to_number(test_data);
    expect(result.toString()).toBe("23.45543");
  });

  test("test 2", () => {
    const test_data = "Harry";
    const result = convert_to_number(test_data);
    expect(result).toBe("Harry");
  });
});

describe("utils/is_time_string", () => {
  test("test 1", () => {
    const test_data = "23:12:11";
    const result = is_time_string(test_data);
    expect(result).toBeTruthy();
  });

  test("test 2", () => {
    const test_data = "23:jk:34";
    const result = is_time_string(test_data);
    expect(result).toBeFalsy();
  });
});

describe("utils/is_year_string", () => {
  test("test 1", () => {
    const test_data = "3456";
    const result = is_year_string(test_data);
    expect(result).toBeTruthy();
  });

  test("test 2", () => {
    const test_data = "34HJ";
    const result = is_year_string(test_data);
    expect(result).toBeFalsy();
  });
});

describe("utils/detect_query_type", () => {
  test("test 1", async () => {
    const query = `
        SELECT 
        student_id
        FROM student_table
        `;

    const result = await detect_query_type(query);
    expect(result).toBe("SELECT");
  });

  test("test 1", async () => {
    const query = `
        DELETE FROM 
        student_table
        WHERE student_id = 2
        `;

    const result = await detect_query_type(query);
    expect(result).toBe("DELETE");
  });

  test("test 1", async () => {
    const query = `
        UPDATE student_table
        SET student_name = 'Prasenjeet Symon'
        WHERE student_id = 2
        `;

    const result = await detect_query_type(query);
    expect(result).toBe("UPDATE");
  });

  test("test 1", async () => {
    const query = `
        CREATE TABLE student_table
        (
            student_name text
        )
        `;

    const result = await detect_query_type(query);
    expect(result).toBe("CREATE");
  });

  test("test 1", async () => {
    const query = `
        INSERT INTO student_table
        (
            student_name
        )
        VALUES
        (
            'Prasenjeet Symon'
        )
        `;

    const result = await detect_query_type(query);
    expect(result).toBe("INSERT");
  });
});
