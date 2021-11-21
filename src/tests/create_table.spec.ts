import { make_select_literals } from "../lib/single-table/select_clause";
import {
  create_table,
  extract_column_info,
  extract_table_name,
  group_bracket_together,
} from "../lib/table-modification/create_table";
import { setup_initial_config_tables } from "../lib/utils";

/**
 * @jest-environment jsdom
 */

describe("create_table/extract_table_name", () => {
  test("test_1", () => {
    const tets_data = ` CREATE TABLE student_table ();`;
    const result = extract_table_name(make_select_literals(tets_data));
    expect(result).toEqual({
      database_name: null,
      table_name: "student_table",
    });
  });

  test("test_2", () => {
    const tets_data = ` CREATE TABLE student_table AS ();`;
    const result = extract_table_name(make_select_literals(tets_data));
    expect(result).toEqual({
      database_name: null,
      table_name: "student_table",
    });
  });

  test("test_3", () => {
    const tets_data = " CREATE TABLE school.`student_table` ();";
    const result = extract_table_name(make_select_literals(tets_data));
    expect(result).toEqual({
      database_name: "school",
      table_name: "student_table",
    });
  });
});

describe("create_table/group_bracket_together", () => {
  test("test_1", async () => {
    const test_data = make_select_literals(` 
        varchar(50),
        varchar(45),
        char(10),
        DATETIME,
        ENUM('Harry', 'Potter', 'age')
        `);

    const expected_result = [
      "varchar",
      "( 50 )",
      ",",
      "varchar",
      "( 45 )",
      ",",
      "char",
      "( 10 )",
      ",",
      "DATETIME",
      ",",
      "ENUM",
      "( 'Harry' , 'Potter' , 'age' )",
    ];
    const result = await group_bracket_together(test_data);
    expect(result).toEqual(expected_result);
  });
});

describe("create_table/extract_column_info", () => {
  test("first_test", async () => {
    const tets_data = ` CREATE TABLE student_table (
            student_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            name text NOT NULL,
            age INT UNSIGNED,
            gender ENUM('male', 'female')
        );
        `;

    const result = await extract_column_info(make_select_literals(tets_data));
    expect(result).toEqual([
      {
        column_name: "student_id",
        column_datatype: { first_part: "BIGINT", second_part: null },
        can_null: true,
        is_unsigned: true,
        default_value: "null",
        is_primary_key: true,
        is_auto_increment: true,
      },
      {
        column_name: "name",
        column_datatype: { first_part: "text", second_part: null },
        can_null: false,
        is_unsigned: false,
        default_value: "null",
        is_primary_key: false,
        is_auto_increment: false,
      },
      {
        column_name: "age",
        column_datatype: { first_part: "INT", second_part: null },
        can_null: true,
        is_unsigned: true,
        default_value: "null",
        is_primary_key: false,
        is_auto_increment: false,
      },
      {
        column_name: "gender",
        column_datatype: {
          first_part: "ENUM",
          second_part: ["'male'", "'female'"],
        },
        can_null: true,
        is_unsigned: false,
        default_value: "null",
        is_primary_key: false,
        is_auto_increment: false,
      },
    ]);
  });
});

describe("create_table/create_table", () => {
  test("create_cofig_tables", async () => {
    await setup_initial_config_tables();
  });

  test("should create table", async () => {
    const test_data = ` 
        CREATE TABLE student_table ( 
            student_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            student_name TEXT,
            mobile_phone_number VARCHAR(12),
            age INT UNSIGNED NOT NULL DEFAULT 12
            gender ENUM('male', 'female', 'other') NOT NULL
        );
        `;

    const result = await create_table(test_data, "test_database");
    expect(result).toBe("OK");
  });
});
