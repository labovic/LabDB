//
// Created by Stefan on 16/03/2023.
//

#ifndef LABDB_QUERY_H
#define LABDB_QUERY_H

#include "pager.h"
#include "database.h"
#include "table.h"

typedef struct query query;
typedef struct join_query join_query;
typedef struct condition condition;
typedef struct column_to_update column_to_update;
union type_value;


enum query_type {SELECT, INSERT, UPDATE, DELETE};
enum relation {EQUAL, NOT_EQUAL, LESS, MORE, LESS_EQUAL, MORE_EQUAL};


struct query {
    enum query_type type;
    char** viewCols;
    char* table;
    condition* condition;
};

struct column_to_update {
    char* col_name;
    enum data_type type;
    uint32_t size;
    union type_value* value;

    column_to_update* next;
};

struct condition {
    char* col_name;
    enum data_type type;
    uint32_t size;
    enum relation relation;
    union type_value* value;

    condition* next;
};

union type_value {
    int32_t i;
    float f;
    bool b;
    char* s;
};

void select_records_from_table(uint8_t num_cols, char** view_cols, condition* condition, table* tb, database* db, FILE* f);
void update_records_in_table(column_to_update* col, condition* cond, table* tb, database* db, FILE* f);
void delete_records_from_table(condition* cond, table* tb, database* db, FILE* f);

condition* create_integer_condition(char* name, enum relation relation, int32_t val);

column_to_update* create_integer_column_to_update(char* name, int32_t val);

#endif //LABDB_QUERY_H
