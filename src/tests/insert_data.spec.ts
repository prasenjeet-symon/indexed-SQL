import BigNumber from "bignumber.js";
import {
  check_the_data_against_table,
  dismental_the_comma_inside_bracket,
  extract_insertion_data,
  extract_insertion_data_column_info,
  insert_data,
} from "../lib/single-table/insert_data";
import { make_select_literals } from "../lib/single-table/select_clause";
import { create_table } from "../lib/table-modification/create_table";
import { setup_initial_config_tables } from "../lib/utils";
/**
 * @jest-environment jsdom
 */
describe("insert_data/extract_insertion_data", () => {
  test("test_1", async () => {
    const test_data = ` INSERT INTO student_table ( student_name, age, gender )
        VALUES
        ( 'Prasenjeet Kumar', 12, 'male'),
        ( 'Divya Devshree Dhillion', 13, 'female' ),
        ( 'Aparna Kumari', 12, 'female' )
        `;

    const expected_values = [
      ["'Prasenjeet Kumar'", new BigNumber("12"), "'male'"],
      ["'Divya Devshree Dhillion'", new BigNumber("13"), "'female'"],
      ["'Aparna Kumari'", new BigNumber("12"), "'female'"],
    ];

    const result = await extract_insertion_data(
      make_select_literals(test_data)
    );
    expect(result).toEqual(expected_values);
  });

  test("test_1", async () => {
    const test_data = ` INSERT INTO student_table ( student_name, age, gender )
        VALUES
        ( 'Prasenjeet Kumar', 12, 'male'),
        ( 'Divya Devshree Dhillion', 13, 'female' ),
        ( 'Aparna       Kumari', 12, NULL )
        `;

    const expected_values = [
      ["'Prasenjeet Kumar'", new BigNumber("12"), "'male'"],
      ["'Divya Devshree Dhillion'", new BigNumber("13"), "'female'"],
      ["'Aparna       Kumari'", new BigNumber("12"), "NULL"],
    ];

    const result = await extract_insertion_data(
      make_select_literals(test_data)
    );
    expect(result).toEqual(expected_values);
  });
});

describe("insert_data/dismental_the_comma_inside_bracket", () => {
  test("test 1", () => {
    const test_data = ` ( 'Harry', 'Marry', 12 ) , `;
    const expected_values = [
      "(",
      "'Harry'",
      "_,_",
      "'Marry'",
      "_,_",
      "12",
      ")",
      ",",
    ];
    const result = dismental_the_comma_inside_bracket(
      make_select_literals(test_data)
    );
    expect(result).toEqual(expected_values);
  });
});

describe("insert_data/extract_insertion_data_column_info", () => {
  test("test 1", () => {
    const test_data = `INSERT INTO test_table ( test_id, test_name, date_created , date_updated)
        VALUES
        ( 1, 'test 1' , '2020-09-23 12:30:00', '2020-12-23 11:22:34' );
        `;

    const expected_values = [
      "test_id",
      "test_name",
      "date_created",
      "date_updated",
    ];
    const result = extract_insertion_data_column_info(
      make_select_literals(test_data)
    );
    expect(result).toEqual(expected_values);
  });
});

describe("insert_data/insert_data", () => {
  test("create_cofig_tables", async () => {
    await setup_initial_config_tables();
  });

  test("should create table", async () => {
    const test_data = ` 
        CREATE TABLE student_table ( 
            student_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            student_name TEXT,
            mobile_phone_number VARCHAR(12),
            age INT UNSIGNED NOT NULL DEFAULT 12,
            gender ENUM('male', 'female', 'other') NOT NULL,
            date_created DATETIME
        );
        `;

    const result = await create_table(test_data, "test_database");
    expect(result).toBe("OK");
  });

  test("test the foreign data against table", async () => {
    const test_data = [
      {
        student_id: 1,
        student_name: "Rim'a Symon",
        mobile_phone_number: 8083371360,
        age: 13,
        gender: "male",
        date_created: "2020-02-14 03:12:12",
      },
      {
        student_id: 1,
        student_name: "Rim'a Symon",
        mobile_phone_number: 8083371360,
        age: 13,
        gender: "male",
        date_created: "2020-02-14 03:12:12",
      },
    ];

    await check_the_data_against_table(
      "test_database",
      "student_table",
      test_data
    );
    expect(1).toBe(1);
  });

  test("should insert the data into table created above", async () => {
    const test_data = `
        INSERT INTO student_table
        ( student_name, mobile_phone_number, age, gender,date_created )
        VALUES 
        ( 'Prasenjeet Kumar', '8083371360', 12, 'male', '2020-02-14 03:12:12'),
        ( 'Ritsh Kumar', '8083371361', 13, 'male', '2020-02-14 03:12:12')
        `;

    const result = await insert_data(test_data, "test_database");
    expect(result).toEqual([1, 2]);
  });
});
