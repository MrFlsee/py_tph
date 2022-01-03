import sys
import tpc_h

DEFAULT_ITER_COUNT = 3
SPARK_MEMORY = 64

def spark_loader():
    #todo
    print("load spark")

def execute_all(iter_count):
    sqls = tpc_h.sqls
    for sql in sqls:
        _execute(sql, iter_count)

def execute(query_num, iter_count):
    _execute(tpc_h.sqls[query_num], iter_count)

def _execute(sql, iter_count):
    print(sql)

def write(execution_avg, iter_count):
    print("write result to something..")

if __name__ == '__main__':
    args = sys.argv
    
    query_num = int(args[1])
    iter_count = args[2] if len(args) > 2 else DEFAULT_ITER_COUNT

    print("======================================================================")
    print(f"\tExecution query number : {query_num} | Iteration count : {iter_count}")
    print("======================================================================")

    spark_loader()
    execute_all(iter_count)

    # execute(int(query_num), iter_count)



