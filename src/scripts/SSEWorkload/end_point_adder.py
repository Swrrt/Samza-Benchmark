import sys
dict = {}
fp = open('./sortSBAll.txt')
fp1 = open('partition1.txt', 'w+')
fp2 = open('partition2.txt', 'w+')
fp3 = open('partition3.txt', 'w+')
fp4 = open('partition4.txt', 'w+')
fp5 = open('partition5.txt', 'w+')
fp6 = open('partition6.txt', 'w+')

Order_No = 0
Tran_Maint_Code = 1
Last_Upd_Time = 3
Last_Upd_Time_Dec = 4
Order_Price = 8
Order_Exec_Vol = 9
Order_Vol = 10
Sec_Code = 11
Trade_Dir = 22

def writeList(textList):
    for text in textList:
        fp.write(text)

text = fp.readline()
curTime = ["093000","093000","093000","093000","093000","093000"]
while text:
    textArr = text.split("|")
    time = textArr[Last_Upd_Time].replace(":", "")
    if time >= "093000":
        if int(textArr[Sec_Code]) % 6 == 0:
            if time != curTime[0]:
                curTime[0] = time
                fp1.write("end\n")
            fp1.write(text)
        elif int(textArr[Sec_Code]) % 6 == 1:
            if time != curTime[1]:
                curTime[1] = time
                fp2.write("end\n")
            fp2.write(text)
        elif int(textArr[Sec_Code]) % 6 == 2:
            if time != curTime[2]:
                curTime[2] = time
                fp3.write("end\n")
            fp3.write(text)
        elif int(textArr[Sec_Code]) % 6 == 3:
            if time != curTime[2]:
                curTime[2] = time
                fp4.write("end\n")
            fp4.write(text)
        elif int(textArr[Sec_Code]) % 6 == 4:
            if time != curTime[2]:
                curTime[2] = time
                fp5.write("end\n")
            fp5.write(text)
        elif int(textArr[Sec_Code]) % 6 == 5:
            if time != curTime[2]:
                curTime[2] = time
                fp6.write("end\n")
            fp6.write(text)
        else:
            print "error"
            sys.exit()
    text = fp.readline()
fp.close()
fp1.close()
fp2.close()
fp3.close()
fp4.close()
fp5.close()
fp6.close()
