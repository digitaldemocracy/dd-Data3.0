#CA 80 A
#CA 40 S
#TX 150 A
#TX 31 S
#FL 120 A
#FL 40 S
#NY 150 A
#NY 63 S

GET_OLD_COUNTS = """select count from DataWarehousing 
                               where state = %(state)s 
                               and type = %(type)s"""

CHECK_LEGISLATORS = """select count(*) from Term 
                           where state = %(state)s 
                           and house = %(house)s
                           and current_term = 1"""

CHECK_COMMITTEES = """select count(*) from Committee 
                           where state = %(state)s
                           and current_flag = 1"""

CHECK_SERVESON = """select count(*) from servesOn 
                           where state = %(state)s
                           and current_flag = 1"""

UPDATE = """update Datawarehousing set count = %(count)s
                        where type = %(type)s and state = %(state)s"""
