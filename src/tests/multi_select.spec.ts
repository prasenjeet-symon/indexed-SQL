import {
  extract_columns_info_of_multi_select,
  find_alies_column_name,
  is_data_source_union,
  is_single_select_datasource,
  multiple_select,
  multiple_select_data_source,
  perform_join,
  retrive_base_source_data,
} from "../lib/multiple-table/multi_select";
import { insert_data } from "../lib/single-table/insert_data";
import { make_select_literals } from "../lib/single-table/select_clause";
import { create_table } from "../lib/table-modification/create_table";
import { setup_initial_config_tables } from "../lib/utils";

/**
 * @jest-environment jsdom
 */

describe("multi_select/multiple_select_data_source", () => {
  test("test 1", async () => {
    const test_data = `
        SELECT

        s.student_id,
        s.mobile_number,
        a.pincode,
        a.street 

        FROM student_table as s
        LEFT OUTER JOIN address_table a
        ON a.student_id = s.student_id
        RIGHT OUTER JOIN school_table sc
        ON sc.school_id= s.school_id
        WHERE s.student_id > 2
        `;
    const result = multiple_select_data_source(make_select_literals(test_data));

    const expected_output = [
      { type: "data_source", data: ["student_table", "as", "s"] },
      { type: "join_type", data: "LEFT OUTER JOIN" },
      { type: "data_source", data: ["address_table", "a"] },
      { type: "on_condition", data: ["a.student_id", "=", "s.student_id"] },
      { type: "join_type", data: "RIGHT OUTER JOIN" },
      { type: "data_source", data: ["school_table", "sc"] },
      { type: "on_condition", data: ["sc.school_id", "=", "s.school_id"] },
    ];

    expect(result).toEqual(expected_output);
  });

  test("test 2", async () => {
    const test_data = `
        SELECT

        s.student_id,
        s.mobile_number,
        a.pincode,
        a.street 

        FROM student_table as s
        LEFT OUTER JOIN address_table a
        ON a.student_id = s.student_id
        RIGHT OUTER JOIN
        ( 
            SELECT school_id, school_name, address
            FROM school_table
            WHERE school_id > 3 AND school_id < 12
         ) sc
        ON sc.school_id= s.school_id
        WHERE s.student_id > 2
        `;
    const result = multiple_select_data_source(make_select_literals(test_data));
    const expected_output = [
      { type: "data_source", data: ["student_table", "as", "s"] },
      { type: "join_type", data: "LEFT OUTER JOIN" },
      { type: "data_source", data: ["address_table", "a"] },
      { type: "on_condition", data: ["a.student_id", "=", "s.student_id"] },
      { type: "join_type", data: "RIGHT OUTER JOIN" },
      {
        type: "data_source",
        data: make_select_literals(`( 
                SELECT school_id, school_name, address
                FROM school_table
                WHERE school_id > 3 AND school_id < 12
             ) sc`),
      },
      { type: "on_condition", data: ["sc.school_id", "=", "s.school_id"] },
    ];

    expect(result).toEqual(expected_output);
  });
});

describe("multi_select/find_alies_column_name", () => {
  test("test 1", async () => {
    const test_data = ` first_name as f_name`;
    const result = find_alies_column_name(make_select_literals(test_data));
    expect(result).toBe("f_name");
  });

  test("test 2", async () => {
    const test_data = ` first_name  f_name`;
    const result = find_alies_column_name(make_select_literals(test_data));
    expect(result).toBe("f_name");
  });
});

describe("multi_select/extract_columns_info_of_multi_select", () => {
  test("test 1", async () => {
    const test_data = `
        SELECT 

        s.full_name as f_name,
        s.mobile_number cell_number,
        s.gender s_gender,
        ad.pincode pin,
        ad.country country,
        ad.state state

        FROM student_table as s
        LEFT OUTER JOIN address_table as ad
        ON s.student_id = ad.student_id
        WHERE s.student_id > 2 AND s.student_id < 20
        `;

    const expected_output = [
      { original_column_name: "s.full_name", final_column_name: "f_name" },
      {
        original_column_name: "s.mobile_number",
        final_column_name: "cell_number",
      },
      { original_column_name: "s.gender", final_column_name: "s_gender" },
      { original_column_name: "ad.pincode", final_column_name: "pin" },
      { original_column_name: "ad.country", final_column_name: "country" },
      { original_column_name: "ad.state", final_column_name: "state" },
    ];

    const result = extract_columns_info_of_multi_select(
      make_select_literals(test_data)
    );
    expect(result).toEqual(expected_output);
  });

  test("test 1", async () => {
    const test_data = `
        SELECT 

        s.full_name as f_name,
        s.mobile_number as cell_number,
        s.gender as s_gender,
        ad.pincode as pin,
        ad.country as country,
        ad.state as state

        FROM student_table as s
        LEFT OUTER JOIN address_table as ad
        ON s.student_id = ad.student_id
        WHERE s.student_id > 2 AND s.student_id < 20
        `;

    const expected_output = [
      { original_column_name: "s.full_name", final_column_name: "f_name" },
      {
        original_column_name: "s.mobile_number",
        final_column_name: "cell_number",
      },
      { original_column_name: "s.gender", final_column_name: "s_gender" },
      { original_column_name: "ad.pincode", final_column_name: "pin" },
      { original_column_name: "ad.country", final_column_name: "country" },
      { original_column_name: "ad.state", final_column_name: "state" },
    ];

    const result = extract_columns_info_of_multi_select(
      make_select_literals(test_data)
    );
    expect(result).toEqual(expected_output);
  });
});

describe("multi_select/is_data_source_union", () => {
  test("test 1", async () => {
    const test_data = `
        (
            (
                SELECT full_name, gender, age
                FROM student_1_table
                WHERE student_id = 2
            )
            union ALL
            (
                SELECT full_name, gender, age
                FROM student_2_table
                WHERE student_id = 21
            )
            UNION
            (
                SELECT full_name, gender, age
                FROM student_3_table
                WHERE student_id = 23
            )
        ) as fg
        `;

    const result = await is_data_source_union(make_select_literals(test_data));
    expect(result).toBeTruthy();
  });

  test("test 2", async () => {
    const test_data = `
        ((
            SELECT full_name, age, gender, mother_name
            FROM student_table
            WHERE student_id > 12 AND gender = 'male'
        )) as hu 
        `;
    const result = await is_data_source_union(make_select_literals(test_data));
    expect(result).toBeFalsy();
  });

  test("test 3", async () => {
    const test_data = `
        student_table as st
        `;
    const result = await is_data_source_union(make_select_literals(test_data));
    expect(result).toBeFalsy();
  });

  test("test 4", async () => {
    const test_data = `
        (( student_table )) as st
        `;
    const result = await is_data_source_union(make_select_literals(test_data));
    expect(result).toBeFalsy();
  });
});

describe("multi_select/is_single_select_datasource", () => {
  test("test 1", async () => {
    const test_data = `
        (
            (
                SELECT full_name, gender, age
                FROM student_1_table
                WHERE student_id = 2
            )
            union ALL
            (
                SELECT full_name, gender, age
                FROM student_2_table
                WHERE student_id = 21
            )
            UNION
            (
                SELECT full_name, gender, age
                FROM student_3_table
                WHERE student_id = 23
            )
        ) as fg
        `;

    const result = await is_single_select_datasource(
      make_select_literals(test_data)
    );
    expect(result).toBeFalsy();
  });

  test("test 2", async () => {
    const test_data = `
        ((
            SELECT full_name, age, gender, mother_name
            FROM student_table
            WHERE student_id > 12 AND gender = 'male'
        )) as hu 
        `;
    const result = await is_single_select_datasource(
      make_select_literals(test_data)
    );
    expect(result).toBeTruthy();
  });

  test("test 3", async () => {
    const test_data = `
        student_table as st
        `;
    const result = await is_single_select_datasource(
      make_select_literals(test_data)
    );
    expect(result).toBeFalsy();
  });

  test("test 4", async () => {
    const test_data = `
        (( student_table )) as st
        `;
    const result = await is_single_select_datasource(
      make_select_literals(test_data)
    );
    expect(result).toBeFalsy();
  });
});

describe("multi_select/retrive_base_source_data", () => {
  test("create_cofig_tables", async () => {
    await setup_initial_config_tables();
  });

  test("should create table", async () => {
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

  test("should insert the data into student_table", async () => {
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

  test("should retrive the inserted data", async () => {
    const test_data = ` student_table as st `;
    const result = await retrive_base_source_data(
      make_select_literals(test_data),
      "test_database"
    );
    const expected_output = [
      {
        "st.student_id": 1,
        "st.student_name": "'Prasenjeet Kumar'",
        "st.mobile_phone_number": "'8083371360'",
        "st.age": "12",
        "st.gender": "'male'",
      },
      {
        "st.student_id": 2,
        "st.student_name": "'Ritsh Kumar'",
        "st.mobile_phone_number": "'8083371361'",
        "st.age": "13",
        "st.gender": "'male'",
      },
    ];
    expect(result).toEqual(expected_output);
  });
});

describe("multi_select/perform_join", () => {
  const test_left_table = [
    {
      "s.student_id": 1,
      "s.name": "Prasenjeet Kumar",
      "s.age": 12,
      "s.gender": "male",
    },
    {
      "s.student_id": 2,
      "s.name": "Rohan Singh",
      "s.age": 11,
      "s.gender": "male",
    },
    {
      "s.student_id": 3,
      "s.name": "Divya Devshree Dhillion",
      "s.age": 10,
      "s.gender": "female",
    },
    {
      "s.student_id": 4,
      "s.name": "Anupam Singh",
      "s.age": 14,
      "s.gender": "female",
    },
  ];

  const test_right_table = [
    {
      "ad.address_id": 1,
      "ad.pincode": 1234,
      "ad.country": "india",
      "ad.student_id": 1,
    },
    {
      "ad.address_id": 2,
      "ad.pincode": 2345,
      "ad.country": "Japan",
      "ad.student_id": 2,
    },
    {
      "ad.address_id": 3,
      "ad.pincode": 4545,
      "ad.country": "China",
      "ad.student_id": 3,
    },
    {
      "ad.address_id": 4,
      "ad.pincode": 6767,
      "ad.country": "USA",
      "ad.student_id": 4,
    },
    {
      "ad.address_id": 5,
      "ad.pincode": 8989,
      "ad.country": "England",
      "ad.student_id": 5,
    },
    {
      "ad.address_id": 6,
      "ad.pincode": 2323,
      "ad.country": "Korea",
      "ad.student_id": 6,
    },
    {
      "ad.address_id": 7,
      "ad.pincode": 1212,
      "ad.country": "USA",
      "ad.student_id": 7,
    },
  ];

  test("test 1", async () => {
    const result = await perform_join(
      test_left_table,
      test_right_table,
      "INNER JOIN",
      ["s.student_id", "=", "ad.student_id"]
    );

    const expected_output = [
      {
        "s.student_id": 1,
        "s.name": "Prasenjeet Kumar",
        "s.age": 12,
        "s.gender": "male",
        "ad.address_id": 1,
        "ad.pincode": 1234,
        "ad.country": "india",
        "ad.student_id": 1,
      },
      {
        "s.student_id": 2,
        "s.name": "Rohan Singh",
        "s.age": 11,
        "s.gender": "male",
        "ad.address_id": 2,
        "ad.pincode": 2345,
        "ad.country": "Japan",
        "ad.student_id": 2,
      },
      {
        "s.student_id": 3,
        "s.name": "Divya Devshree Dhillion",
        "s.age": 10,
        "s.gender": "female",
        "ad.address_id": 3,
        "ad.pincode": 4545,
        "ad.country": "China",
        "ad.student_id": 3,
      },
      {
        "s.student_id": 4,
        "s.name": "Anupam Singh",
        "s.age": 14,
        "s.gender": "female",
        "ad.address_id": 4,
        "ad.pincode": 6767,
        "ad.country": "USA",
        "ad.student_id": 4,
      },
    ];

    expect(result).toEqual(expected_output);
  });

  test("left_outer_join", async () => {
    const result = await perform_join(
      test_left_table,
      test_right_table,
      "LEFT OUTER JOIN",
      ["s.student_id", "=", "ad.student_id"]
    );

    const expected_output = [
      {
        "s.student_id": 1,
        "s.name": "Prasenjeet Kumar",
        "s.age": 12,
        "s.gender": "male",
        "ad.address_id": 1,
        "ad.pincode": 1234,
        "ad.country": "india",
        "ad.student_id": 1,
      },
      {
        "s.student_id": 2,
        "s.name": "Rohan Singh",
        "s.age": 11,
        "s.gender": "male",
        "ad.address_id": 2,
        "ad.pincode": 2345,
        "ad.country": "Japan",
        "ad.student_id": 2,
      },
      {
        "s.student_id": 3,
        "s.name": "Divya Devshree Dhillion",
        "s.age": 10,
        "s.gender": "female",
        "ad.address_id": 3,
        "ad.pincode": 4545,
        "ad.country": "China",
        "ad.student_id": 3,
      },
      {
        "s.student_id": 4,
        "s.name": "Anupam Singh",
        "s.age": 14,
        "s.gender": "female",
        "ad.address_id": 4,
        "ad.pincode": 6767,
        "ad.country": "USA",
        "ad.student_id": 4,
      },
    ];

    expect(result).toEqual(expected_output);
  });

  test("right_outer_join", async () => {
    const expected_output = [
      {
        "s.student_id": 1,
        "s.name": "Prasenjeet Kumar",
        "s.age": 12,
        "s.gender": "male",
        "ad.address_id": 1,
        "ad.pincode": 1234,
        "ad.country": "india",
        "ad.student_id": 1,
      },
      {
        "s.student_id": 2,
        "s.name": "Rohan Singh",
        "s.age": 11,
        "s.gender": "male",
        "ad.address_id": 2,
        "ad.pincode": 2345,
        "ad.country": "Japan",
        "ad.student_id": 2,
      },
      {
        "s.student_id": 3,
        "s.name": "Divya Devshree Dhillion",
        "s.age": 10,
        "s.gender": "female",
        "ad.address_id": 3,
        "ad.pincode": 4545,
        "ad.country": "China",
        "ad.student_id": 3,
      },
      {
        "s.student_id": 4,
        "s.name": "Anupam Singh",
        "s.age": 14,
        "s.gender": "female",
        "ad.address_id": 4,
        "ad.pincode": 6767,
        "ad.country": "USA",
        "ad.student_id": 4,
      },
      {
        "ad.address_id": 5,
        "ad.pincode": 8989,
        "ad.country": "England",
        "ad.student_id": 5,
        "s.student_id": null,
        "s.name": null,
        "s.age": null,
        "s.gender": null,
      },
      {
        "ad.address_id": 6,
        "ad.pincode": 2323,
        "ad.country": "Korea",
        "ad.student_id": 6,
        "s.student_id": null,
        "s.name": null,
        "s.age": null,
        "s.gender": null,
      },
      {
        "ad.address_id": 7,
        "ad.pincode": 1212,
        "ad.country": "USA",
        "ad.student_id": 7,
        "s.student_id": null,
        "s.name": null,
        "s.age": null,
        "s.gender": null,
      },
    ];

    const result = await perform_join(
      test_left_table,
      test_right_table,
      "RIGHT OUTER JOIN",
      ["s.student_id", "=", "ad.student_id"]
    );

    expect(result).toEqual(expected_output);
  });
});

describe("multi_select/multiple_select", () => {
  // prepare the data by creating two table
  // 1) student_table
  // 2) address_table
  // insert the data
  // then query both table
  test("create_cofig_tables", async () => {
    await setup_initial_config_tables();
  });

  test("should create table student table", async () => {
    const test_data = ` 
        CREATE TABLE student_table ( 
            student_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            student_name TEXT,
            mobile_phone_number VARCHAR(12),
            age INT UNSIGNED NOT NULL DEFAULT 12,
            gender ENUM('male', 'female', 'other') NOT NULL
        )
        `;

    const result = await create_table(test_data, "test_database_one");
    expect(result).toBe("OK");
  });

  test("should create address_table", async () => {
    const test_data = `
        CREATE TABLE address_table ( 
            address_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            pincode varchar(10),
            state text,
            country text,
            street text,
            student_id BIGINT unsigned
        )
        `;
    const result = await create_table(test_data, "test_database_one");
    expect(result).toBe("OK");
  });

  test("should insert data into student_table", async () => {
    const test_data = `
        INSERT INTO student_table
        ( student_name, mobile_phone_number, age, gender)

        VALUES 

        ( 'Prasenjeet Kumar', '8083371360', 12, 'male'),
        ( 'Ritsh Kumar', '8083371361', 13, 'male'),
        ( 'Rohan Verma', '8989898989', 15, 'male'),
        ( 'Aparna Kumari', '3434343434', 18, 'female'),
        ( 'Divya Devshree', '4545454545', 19, 'female')
        `;

    const result = await insert_data(test_data, "test_database_one");
    expect(result).toEqual([1, 2, 3, 4, 5]);
  });

  test("should insert data into address_table", async () => {
    const test_data = `
        INSERT INTO address_table
        ( pincode, state, country, street, student_id)

        VALUES 

        ( '343434', 'Bihar', 'India', 'Baghbhu Lane', 1  ),
        ( '454545', 'Bihar', 'India', 'Baghbhu Klae Io', 2  ),
        ( '565656', 'Bihar', 'India', 'Jkjshdj Klae Io', 3 ),
        ( '565656', 'Bihar', 'India', 'Hddss Lkkjj Oopp', 4 ),
        ( '898989', 'Bihar', 'India', 'Hddss Lkkjj', 5 ),
        ( '111111', 'Bihar', 'India', 'Hddss', 6 ),
        ( '222222', 'Bihar', 'India', 'Sddff', 7 )
        `;

    const result = await insert_data(test_data, "test_database_one");
    expect(result).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  test("perform multi query", async () => {
    const query = `
        SELECT

        st.student_id, 
        st.student_name,
        st.age,
        st.gender,
        address_table.pincode,
        address_table.country

        FROM student_table as st
        INNER JOIN address_table
        ON st.student_id = address_table.student_id
        WHERE st.student_id > 2
        ORDER BY st.age
        `;

    await multiple_select(query, "test_database_one");
    expect(1).toBe(1);
  });
});
