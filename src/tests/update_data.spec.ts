import BigNumber from "bignumber.js";
import { insert_data } from "../lib/single-table/insert_data";
import {
  make_select_literals,
  single_select,
} from "../lib/single-table/select_clause";
import {
  extract_table_info_update,
  extract_update_column_value_pair,
  extract_update_where_statement,
  update_data,
} from "../lib/single-table/update_data";
import { create_table } from "../lib/table-modification/create_table";
import { setup_initial_config_tables } from "../lib/utils";
/**
 * @jest-environment jsdom
 */
describe("update_data/extract_update_column_value_pair", () => {
  test("test 1", async () => {
    const tets_data = `
        UPDATE student_table
        SET
        student_name = 'Prasenjeet , Kumar',
        age = 12,
        gender = 'other'
        WHERE student_id = 2
        `;

    const result = extract_update_column_value_pair(
      make_select_literals(tets_data)
    );
    const expected_values = {
      column_names: ["student_name", "age", "gender"],
      column_values: ["'Prasenjeet , Kumar'", new BigNumber(12), "'other'"],
    };

    expect(result).toEqual(expected_values);
  });

  test("test 2", async () => {
    const tets_data = `
        UPDATE student_table
        SET
        student_name = 'Prasenjeet Kumar',
        age = 12,
        gender = 'other'
        `;

    const result = extract_update_column_value_pair(
      make_select_literals(tets_data)
    );
    const expected_values = {
      column_names: ["student_name", "age", "gender"],
      column_values: ["'Prasenjeet Kumar'", new BigNumber(12), "'other'"],
    };

    expect(result).toEqual(expected_values);
  });
});

describe("update_data/extract_update_where_statement", () => {
  test("test 1", async () => {
    const test_data = `
        UPDATE student_table
        SET
        student_name = 'Prasenjeet Kumar',
        age = 12,
        gender = 'other'
        WHERE student_id = 2 AND age > 12`;

    const expected_values = make_select_literals(`student_id = 2 AND age > 12`);
    const result = extract_update_where_statement(
      make_select_literals(test_data)
    );
    expect(result).toEqual(expected_values);
  });

  test("test 2", async () => {
    const test_data = `
        UPDATE student_table 
        SET
        student_name = 'Prasenjeet Kumar',
        age = 12,
        gender = 'other'`;

    const expected_values: any = null;
    const result = extract_update_where_statement(
      make_select_literals(test_data)
    );
    expect(result).toEqual(expected_values);
  });
});

describe("update_data/extract_table_info_update", () => {
  test("test 1", () => {
    const test_data = `
        UPDATE student_table 
        SET
        student_name = 'Prasenjeet Kumar',
        age = 12,
        gender = 'other'`;

    const result = extract_table_info_update(make_select_literals(test_data));
    expect(result).toEqual({
      database_name: undefined,
      table_name: "student_table",
    });
  });

  test("test 2", () => {
    const test_data = `
        UPDATE test_database.student_table
        SET
        student_name = 'Prasenjeet Kumar',
        age = 12,
        gender = 'other'
        WHERE student_id = 2 AND age > 12`;

    const result = extract_table_info_update(make_select_literals(test_data));
    expect(result).toEqual({
      database_name: "test_database",
      table_name: "student_table",
    });
  });
});

describe("update_data/update_data", () => {
  test("create_cofig_tables", async () => {
    await setup_initial_config_tables();
  });

  test("create the table named student table", async () => {
    const test_data = ` 
        CREATE TABLE student_table
        ( 
            student_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            student_name TEXT,
            mobile_phone_number VARCHAR(12),
            age INT UNSIGNED NOT NULL DEFAULT 12,
            gender ENUM('male', 'female', 'other') NOT NULL
        )
        `;

    const result = await create_table(test_data, "test_database");
    expect(result).toBe("OK");
  });

  test("insert the data in the student table", async () => {
    const test_data = `
        INSERT INTO student_table
        ( student_name, mobile_phone_number, age, gender)
        VALUES
        ( 'Prasenjeet Kumar', '8083371360', 12, 'male'),
        ( 'Ritsh Kumar', '8083371361', 13, 'male')
        `;

    const result = await insert_data(test_data, "test_database");
    expect(result).toEqual([1, 2]);
  });

  test("update data of the student table", async () => {
    const test_data = ` UPDATE test_database.student_table
        SET
        test_database.student_id = 12,
        test_database.student_name = 'Divya Devshree Dhillion',
        test_database.age = 14,
        test_database.gender = 'female'
        WHERE test_database.student_id = 2
        `;

    const result = await update_data(test_data, "test_database");
    expect(result).toBe("OK");
  });

  test("check for the updated data in the student table", async () => {
    const query = `
        SELECT student_name, mobile_phone_number, age , gender
        FROM student_table
        `;

    const result = await single_select(query, "test_database");
    const expected_values = [
      {
        student_name: "'Prasenjeet Kumar'",
        mobile_phone_number: "'8083371360'",
        age: "12",
        gender: "'male'",
      },
      {
        student_name: "'Divya Devshree Dhillion'",
        mobile_phone_number: "'8083371361'",
        age: "14",
        gender: "'female'",
      },
    ];
    expect(result).toEqual(expected_values);
  });
});
