import { insert_data } from "../lib/single-table/insert_data";
import {
  combine_all_sql_spaced_literals,
  combine_sql_spaced_literals,
  do_data_modification,
  is_data_source_subquery,
  is_select_multiquery,
  make_select_literals,
  parse_columns_to_select,
  parse_data_filter_and_manupulation_part,
  parse_data_source,
  parse_select_clause,
  parse_union_datasource,
  replace_sql_string_with_marker,
  select_given_columns_from_dataset,
  single_select,
} from "../lib/single-table/select_clause";
import { create_table } from "../lib/table-modification/create_table";
import { setup_initial_config_tables } from "../lib/utils";
/**
 * @jest-environment jsdom
 */
test("replace_sql_string_with_marker", async () => {
  const test_string = `sadhkhksah ahdkahka 'hsadj khkad' hhkadhka 'H hdh''ka ahsdka'`;
  const res = replace_sql_string_with_marker(test_string);
  const expected_result = `sadhkhksah ahdkahka  MS_0  hhkadhka  MS_1`;
  expect(res.final_string).toBe(expected_result);
});

test("select_clause/make_select_literals", () => {
  const test_data = ` SELECT name,age, gender FROM test_table WHERE  age =12 AND gender='Male'`;
  const expected_output =
    `SELECT name , age , gender FROM test_table WHERE age = 12 AND gender = 'Male'`.split(
      " "
    );
  const result = make_select_literals(test_data);
  expect(result).toEqual(expected_output);
});

test("select_clause/parse_columns_to_select", () => {
  const test_data = make_select_literals(
    ` SELECT test_database.test_table.name as my_name,age my_age, gender [ my gender] FROM test_table WHERE  age =12 AND gender='Male'  `
  );
  const expected_output = [
    { original_column_name: "name", final_column_name: "my_name" },
    { original_column_name: "age", final_column_name: "my_age" },
    { original_column_name: "gender", final_column_name: "my gender" },
  ];

  const result = parse_columns_to_select(test_data);
  expect(result).toEqual(expected_output);

  const test_data_1 = make_select_literals(
    ` SELECT test_database.test_table.name as my_name,age my_age, gender " my gender" FROM test_table WHERE  age =12 AND gender='Male'  `
  );
  const expected_output_1 = [
    { original_column_name: "name", final_column_name: "my_name" },
    { original_column_name: "age", final_column_name: "my_age" },
    { original_column_name: "gender", final_column_name: "my gender" },
  ];

  const result_1 = parse_columns_to_select(test_data_1);
  expect(result_1).toEqual(expected_output_1);

  const test_data_2 = make_select_literals(
    ` SELECT test_database.test_table.name as my name,age my_age, gender " my gender" FROM test_table WHERE  age =12 AND gender='Male'  `
  );
  const expected_output_2 = [
    { original_column_name: "name", final_column_name: "my" },
    { original_column_name: "age", final_column_name: "my_age" },
    { original_column_name: "gender", final_column_name: "my gender" },
  ];

  const result_2 = parse_columns_to_select(test_data_2);
  expect(result_2).toEqual(expected_output_2);
});

test("select_clause/is_data_source_subquery", () => {
  const test_data = make_select_literals(`
    SELECT * 
     FROM 
     (
         SELECT * 
         FROM test_table
         WHERE id = 2
     ) as test
     WHERE test.name = 'Marry'
    `);
  const result = is_data_source_subquery(test_data);
  expect(result).toBe(true);

  const test_data_1 = make_select_literals(
    ` SELECT name , age FROM test_data WHERE age > 12 `
  );
  const result_1 = is_data_source_subquery(test_data_1);
  expect(result_1).toBe(false);
});

test("select_clause/parse_union_datasource", async () => {
  const test_data = make_select_literals(`
    (( SELECT * FROM user WHERE name = 'Marry' ) )

    UNION

    ( SELECT * FROM (
            ( SELECT * FROM user WHERE name = 'Marry' )

            UNION

            ( SELECT * FROM user WHERE name = 'Marry' )

            UNION ALL

            (SELECT * FROM user WHERE name = 'Marry') 
            
                    ) AS JK 
    WHERE name = 'Marry' )

    UNION ALL

    (SELECT * FROM user WHERE name = 'Marry')
    
    `);

  const result = await parse_union_datasource(test_data);
  const expected_output = [
    {
      type: "source",
      data: make_select_literals(
        `(( SELECT * FROM user WHERE name = 'Marry' ) )`
      ),
    },
    { type: "union", data: "UNION" },
    {
      type: "source",
      data: make_select_literals(`( SELECT * FROM (
                ( SELECT * FROM user WHERE name = 'Marry' )
    
                UNION
    
                ( SELECT * FROM user WHERE name = 'Marry' )
                
                UNION ALL
    
                (SELECT * FROM user WHERE name = 'Marry') 
                
                        ) AS JK 
        WHERE name = 'Marry' )`),
    },
    { type: "union", data: "UNION ALL" },
    {
      type: "source",
      data: make_select_literals(`(SELECT * FROM user WHERE name = 'Marry')`),
    },
  ];

  expect(result).toEqual(expected_output);
});

test("select_clause/parse_data_source", async () => {
  const test_data = make_select_literals(
    ` SELECT * FROM user_table as user WHERE user.name = 'Harry' `
  );
  const result = await parse_data_source(test_data);
  expect(result).toEqual({
    type: "base",
    database_name: null,
    table_name: "user_table",
  });

  // tets 2
  const test_data_1 = make_select_literals(`
     SELECT * 
     FROM 
     (
         SELECT * 
         FROM test_table
         WHERE id = 2
     ) as test
     WHERE test.name = 'Marry'
    `);

  const result_1 = await parse_data_source(test_data_1);
  expect(result_1).toEqual({
    type: "single_select",
    source_literals: make_select_literals(`
         SELECT * 
         FROM test_table
         WHERE id = 2
    `),
  });

  // test 3
  const test_data_2 = make_select_literals(`
    
    SELECT * 
    FROM 
    (
        (
            SELECT * FROM user WHERE user_id = 2
        )
        UNION ALL
        (
            SELECT * FROM exam_table WHERE exam_id = 3
        )
    ) as test_table
    WHERE test_table.test_id = 4

    `);

  const result_2 = await parse_data_source(test_data_2);

  expect(result_2).toEqual({
    type: "union",
    source_literals: await parse_union_datasource(
      make_select_literals(`
    (
        SELECT * FROM user WHERE user_id = 2
    )
    UNION ALL
    (
        SELECT * FROM exam_table WHERE exam_id = 3
    )`)
    ),
  });
});

test("select_clause/combine_sql_spaced_literals", () => {
  const test_data = ["ORDER", "BY", "NAME", "GAME", "FAME", "SAME", "ON"];
  const expected_output = ["ORDER BY", "NAME", "GAME", "FAME", "SAME", "ON"];
  const result = combine_sql_spaced_literals("ORDER BY", test_data);
  expect(result).toEqual(expected_output);
});

test("select_clause/combine_all_sql_spaced_literals", () => {
  const test_data = ["ORDER", "BY", "GROUP", "BY", "WHERE", "LIMIT", "OFFSET"];
  const expected_output = ["ORDER BY", "GROUP BY", "WHERE", "LIMIT", "OFFSET"];
  const result = combine_all_sql_spaced_literals(test_data);
  expect(result).toEqual(expected_output);
});

test("select_clause/parse_data_filter_and_manupulation_part", () => {
  const test_data = make_select_literals(`
    SELECT * 
    FROM user_table
    WHERE user_name = 'Harry' AND age <= 14
    ORDER BY age|ASC
    GROUP BY age
    OFFSET 2 
    LIMIT 10
    `);

  const result = parse_data_filter_and_manupulation_part(test_data);
  expect(result).toEqual([
    {
      type: "WHERE",
      data: make_select_literals(`WHERE user_name = 'Harry' AND age <= 14`),
    },
    {
      type: "ORDER BY",
      data: combine_sql_spaced_literals(
        "ORDER BY",
        make_select_literals(`ORDER BY age|ASC`)
      ),
    },
    {
      type: "GROUP BY",
      data: combine_sql_spaced_literals(
        "GROUP BY",
        make_select_literals(`GROUP BY age`)
      ),
    },
    { type: "OFFSET", data: make_select_literals(`OFFSET 2 `) },
    { type: "LIMIT", data: make_select_literals(`LIMIT 10`) },
  ]);

  // second test
  const test_data_1 = make_select_literals(`
    SELECT * FROM user_table as us
    WHERE age IN ( SELECT age FROM student_table WHERE class = 3 AND age < 12 ORDER BY age )
    GROUP BY age
    ORDER BY age
    OFFSET 3
    LIMIT 20
    `);

  const result_1 = parse_data_filter_and_manupulation_part(test_data_1);
  expect(result_1).toEqual([
    {
      type: "WHERE",
      data: combine_all_sql_spaced_literals(
        make_select_literals(
          `WHERE age IN ( SELECT age FROM student_table WHERE class = 3 AND age < 12 ORDER BY age )`
        )
      ),
    },
    {
      type: "GROUP BY",
      data: combine_all_sql_spaced_literals(
        make_select_literals(`GROUP BY age`)
      ),
    },
    {
      type: "ORDER BY",
      data: combine_all_sql_spaced_literals(
        make_select_literals(`ORDER BY age`)
      ),
    },
    {
      type: "OFFSET",
      data: combine_all_sql_spaced_literals(make_select_literals(`OFFSET 3`)),
    },
    {
      type: "LIMIT",
      data: combine_all_sql_spaced_literals(make_select_literals(`LIMIT 20`)),
    },
  ]);

  // tets 3
  const test_data_2 = make_select_literals(`
    SELECT
    s.full_name,
    s.age,
    ad.pincode

    FROM user_table as s
    LEFT OUTER JOIN address_table as ad
    ON ad.user_id = s.user_id
    RIGHT OUTER JOIN money_table as mt
    ON mt.money_id = ad.money_id

    WHERE age IN ( SELECT age FROM student_table WHERE class = 3 AND age < 12 ORDER BY age )
    GROUP BY age
    ORDER BY age
    OFFSET 3
    LIMIT 20
    `);

  const result_2 = parse_data_filter_and_manupulation_part(test_data_2);
  expect(result_2).toEqual([
    {
      type: "WHERE",
      data: combine_all_sql_spaced_literals(
        make_select_literals(
          `WHERE age IN ( SELECT age FROM student_table WHERE class = 3 AND age < 12 ORDER BY age )`
        )
      ),
    },
    {
      type: "GROUP BY",
      data: combine_all_sql_spaced_literals(
        make_select_literals(`GROUP BY age`)
      ),
    },
    {
      type: "ORDER BY",
      data: combine_all_sql_spaced_literals(
        make_select_literals(`ORDER BY age`)
      ),
    },
    {
      type: "OFFSET",
      data: combine_all_sql_spaced_literals(make_select_literals(`OFFSET 3`)),
    },
    {
      type: "LIMIT",
      data: combine_all_sql_spaced_literals(make_select_literals(`LIMIT 20`)),
    },
  ]);
});

test("select_clause/parse_select_clause", async () => {
  const test_data = ` 
    SELECT 
    name,
    age,
    gender,
    class,
    section,
    school_id,
    passion
    FROM student_table
    WHERE student_id = > 12 AND gender = 'Male'
    GROUP BY age
    ORDER BY age
    OFFSET 3
    LIMIT 10
    `;

  const result = await parse_select_clause(test_data);
  expect(result).toEqual({
    select_columns_parts: parse_columns_to_select(
      make_select_literals(test_data)
    ),
    select_datasource: await parse_data_source(make_select_literals(test_data)),
    data_manipulation_parts: parse_data_filter_and_manupulation_part(
      make_select_literals(test_data)
    ),
  });
});

test("select_clause/select_given_columns_from_dataset", () => {
  const test_data = [
    { name: "Harry", age: 12, gender: "male", birth: "2020-12-12" },
    { name: "Jerry", age: 13, gender: "female", birth: "2018-02-02" },
    { name: "Kerry", age: 15, gender: "other", birth: "2014-05-09" },
    { name: "Serry", age: 10, gender: "other", birth: "2012-09-23" },
  ];

  const result = select_given_columns_from_dataset(test_data, [
    { original_column_name: "name", final_column_name: "full_name" },
    { original_column_name: "age", final_column_name: "student_age" },
  ]);
  expect(result).toEqual([
    { full_name: "Harry", student_age: 12 },
    { full_name: "Jerry", student_age: 13 },
    { full_name: "Kerry", student_age: 15 },
    { full_name: "Serry", student_age: 10 },
  ]);

  // test 2
  const test_data_1 = [
    { name: "Harry", age: 12, gender: "male", birth: "2020-12-12" },
    { name: "Jerry", age: 13, gender: "female", birth: "2018-02-02" },
    { name: "Kerry", age: 15, gender: "other", birth: "2014-05-09" },
    { name: "Serry", age: 10, gender: "other", birth: "2012-09-23" },
  ];

  const result_1 = select_given_columns_from_dataset(test_data_1, [
    { original_column_name: "friend", final_column_name: "mate" },
    { original_column_name: "age", final_column_name: "student_age" },
  ]);
  expect(result_1).toBeNull();
});

describe("select_clause/do_data_modification", () => {
  test("test 1", async () => {
    const sql_String = `
        SELECT 
        s.student_id
        FROM student_table as s
        WHERE s.student_id > 2
        ORDER BY s.age
        OFFSET 2
        LIMIT 4
        `;
    const manupulation_config = parse_data_filter_and_manupulation_part(
      make_select_literals(sql_String)
    );

    const data = [
      { student_id: 1, age: 11 },
      { student_id: 2, age: 12 },
      { student_id: 3, age: 10 },
      { student_id: 4, age: 9 },
      { student_id: 5, age: 8 },
      { student_id: 6, age: 14 },
      { student_id: 7, age: 18 },
      { student_id: 8, age: 16 },
      { student_id: 9, age: 13 },
    ];

    const result = await do_data_modification(data, manupulation_config);
    expect(result).toEqual([
      { student_id: 3, age: 10 },
      { student_id: 9, age: 13 },
      { student_id: 6, age: 14 },
      { student_id: 8, age: 16 },
    ]);
  });

  test("tets 2", async () => {
    const sql_String = `
        SELECT 
        s.student_id
        FROM student_table as s

        WHERE s.student_id > 2
        ORDER BY s.age
        OFFSET 2
        LIMIT 4
        `;
    const manupulation_config = parse_data_filter_and_manupulation_part(
      make_select_literals(sql_String)
    );

    const data = [
      { "s.student_id": 1, "s.age": 11 },
      { "s.student_id": 2, "s.age": 12 },
      { "s.student_id": 3, "s.age": 10 },
      { "s.student_id": 4, "s.age": 9 },
      { "s.student_id": 5, "s.age": 8 },
      { "s.student_id": 6, "s.age": 14 },
      { "s.student_id": 7, "s.age": 18 },
      { "s.student_id": 8, "s.age": 16 },
      { "s.student_id": 9, "s.age": 13 },
    ];

    const result = await do_data_modification(data, manupulation_config, true);
    expect(result).toEqual([
      { "s.student_id": 3, "s.age": 10 },
      { "s.student_id": 9, "s.age": 13 },
      { "s.student_id": 6, "s.age": 14 },
      { "s.student_id": 8, "s.age": 16 },
    ]);
  });
});

// test for the main query

describe("select_clause/single_select", () => {
  test("create_cofig_tables", async () => {
    await setup_initial_config_tables();
  });

  // prepare the table and data
  test("prepare data", async () => {
    // create the table student table for the test
    const table_student = ` 
        CREATE TABLE student_table ( 
            student_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            student_name TEXT,
            mobile_phone_number VARCHAR(12),
            age INT UNSIGNED NOT NULL DEFAULT 12,
            gender ENUM('male', 'female', 'other') NOT NULL
        );
        `;
    await create_table(table_student, "test_database");

    // insert the data into student table
    const data_to_insert = `
        INSERT INTO student_table
        ( student_name, mobile_phone_number, age, gender) 
        VALUES 
        ( 'Prasenjeet Kumar', '1212121212', 12, 'male'),
        ( 'Ritsh Kumar', '2323232323', 13, 'male'),
        ( 'Divya Devshree Dhillion', '3434343434', 14, 'female'),
        ( 'Rohan Verma', '4545454545', 18, 'male'),
        ( 'Sanjeet Kumar', '5656565656', 21, 'male'),
        ( 'Aparna Kumari', '7878787878', 13, 'female'),
        ( 'Prem Kumar', '8989898989', 19, 'male'),
        ( 'Rima kumari', '9090909090', 22, 'female')
        `;

    await insert_data(data_to_insert, "test_database");
  });

  test("should_retrive_the_data", async () => {
    const single_select_query = `
        SELECT
        student_id,
        student_name,
        gender,
        age

        FROM student_table as st
        WHERE student_table.student_id > 0
        ORDER BY st.age ASC
        OFFSET 2
        LIMIT 4
        `;

    const expected_output = [
      {
        student_id: 6,
        student_name: "'Aparna Kumari'",
        gender: "'female'",
        age: "13",
      },
      {
        student_id: 3,
        student_name: "'Divya Devshree Dhillion'",
        gender: "'female'",
        age: "14",
      },
      {
        student_id: 4,
        student_name: "'Rohan Verma'",
        gender: "'male'",
        age: "18",
      },
      {
        student_id: 7,
        student_name: "'Prem Kumar'",
        gender: "'male'",
        age: "19",
      },
    ];

    const result = await single_select(single_select_query, "test_database");

    expect(result).toEqual(expected_output);
  });
});

describe("select_clause/is_select_multiquery", () => {
  test("test 1", async () => {
    const query = `
        SELECT 
        student_id,
        age,
        gender
        FROM student_table
        WHERE student_id > 2
        `;

    const result = is_select_multiquery(make_select_literals(query));
    expect(result).toBeFalsy();
  });

  test("test 2", async () => {
    const query = `
        SELECT
        st.student_id 
        FROM student_table as st
        LEFT OUTER JOIN address_table as ad
        ON ad.student_id = st.student_id
        WHERE st.student_id  > 3
        `;

    const result = is_select_multiquery(make_select_literals(query));
    expect(result).toBeTruthy();
  });

  test("test 3", async () => {
    const query = `
        SELECT
        st.student_id
        FROM
        (
            SELECT 
            FROM student_table as st
            LEFT OUTER JOIN address_table as ad
            ON ad.student_id = st.student_id
            WHERE st.student_id  > 3
        ) as st
        INNER JOIN address_table as ad
        ON ad.student_id = st.student_id
        WHERE st.student_id  > 3
        `;

    const result = is_select_multiquery(make_select_literals(query));
    expect(result).toBeTruthy();
  });

  test("test 4", async () => {
    const query = `
        SELECT
        st.student_id
        FROM
        (
            SELECT 
            FROM student_table as st
            LEFT OUTER JOIN address_table as ad
            ON ad.student_id = st.student_id
            WHERE st.student_id  > 3
        ) as st
        WHERE st.student_id  > 3
        `;

    const result = is_select_multiquery(make_select_literals(query));
    expect(result).toBeFalsy();
  });
});
