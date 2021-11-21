import {
  delete_data,
  parse_delete_database_table_info,
} from "../lib/single-table/delete_data";
import { insert_data } from "../lib/single-table/insert_data";
import {
  make_select_literals,
  single_select,
} from "../lib/single-table/select_clause";
import { create_table } from "../lib/table-modification/create_table";
import { setup_initial_config_tables } from "../lib/utils";
/**
 * @jest-environment jsdom
 */
describe("delete_data/parse_delete_database_table_info", () => {
  test("test 1", async () => {
    const query = `
        DELETE FROM test_database.student_table
        WHERE test_database.student_table.student_id = 2
        `;
    const result = parse_delete_database_table_info(
      make_select_literals(query)
    );
    expect(result).toEqual({
      database_name: "test_database",
      table_name: "student_table",
    });
  });
});

describe("delete_data/delete_data", () => {
  test("create_cofig_tables", async () => {
    await setup_initial_config_tables();
  });

  test("create table named student table", async () => {
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

  test("insert the data into student table", async () => {
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

  test("should delete the data from the student table", async () => {
    const query = `
        DELETE FROM student_table
        WHERE student_id = 2
        `;
    const result = await delete_data(query, "test_database");
    expect(result).toBe("OK");
  });

  test("check for the deleted data in the student table", async () => {
    const query = `
        SELECT
        student_name,
        mobile_phone_number,
        age,
        gender
        FROM student_table
        `;

    const result = await single_select(query, "test_database");
    const expected_value = [
      {
        student_name: "'Prasenjeet Kumar'",
        mobile_phone_number: "'8083371360'",
        age: "12",
        gender: "'male'",
      },
    ];
    expect(result).toEqual(expected_value);
  });
});
