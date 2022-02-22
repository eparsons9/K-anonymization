import sys
import pandas as pd 
import math
import time

preCount = 0


def main():
    start = time.time()
    path = sys.argv[1]
    k = sys.argv[2]
    colnames = ["Age", "Occupation", "Race", "Sex", "HoursPerWeek", "EducationNum"]
    dataFrame = pd.read_csv(path, names= colnames)
    dataFrame.drop(index=dataFrame.index[0], axis=0, inplace=True)
    #sorted to ensure most alike groups are already together
    df = dataFrame.sort_values(["Age", "Occupation", "Race", "Sex", "HoursPerWeek"])
    

    #add a column representing group number with all x's to dataframe
    df["GroupNum"]='x'
    df = df.reset_index(drop=True)


    #preliminary group number assignment- if values are the same, we don't need 
    #to anonymize the group, so assign group numbers to all groups with same values
    #works because sorted first
    count=0
    for i in range(len(df)-1) :
        row = [df['Age'][i], df['Occupation'][i], df['Race'][i], df['Sex'][i], df['HoursPerWeek'][i]]
        nextRow = [df['Age'][i+1], df['Occupation'][i+1], df['Race'][i+1], df['Sex'][i+1], df['HoursPerWeek'][i+1]]
    
        
        if (row==nextRow):
            df.at[i,'GroupNum'] = count
            df.at[i+1,'GroupNum'] = count
        else:
            count+=1


    
    grouped= df.groupby("GroupNum").size().to_frame()
    #grouped is a DF with group num and size of group num
    grouped.to_csv('fun.csv')
    if(int(k)==2 or int(k)==3):
        countsMasked=grouped[0]<=int(k)
    else:
        countsMasked=grouped[0]<=(int(k)-1)
    counts=grouped[countsMasked]
    #counts is all group nums of size <=k
    counts.to_csv("new.csv")
    df.to_csv("hello.csv")

    
    counter=0
    #loop through and assign groups to x using nearby groups
    for i in range(len(df)):
        if len(counts)>0:
            if df.at[i,'GroupNum']=='x':
                counter+=1
                row_1=counts.axes[0][0] #group num
                row_2=counts.iloc[0][0] #group size
                df.at[i, 'GroupNum']=row_1 #set x equal to a group with counts<k
                counts.iloc[0][0]=row_2+1 #increase group size
                if((row_2+1>=int(k) and int(k)>5)): #if group is big enough, remove it from counts
                    counts = counts.iloc[1: , : ]
                if int(k)==2 and counter==5: #had to hard code for smaller values of k 
                    counts = counts.iloc[1: , : ]
                    counter=0
                if int(k)==3 and counter==4:
                    counts = counts.iloc[1: , : ]
                    counter=0
                if int(k)==4 and counter==4:
                    counts = counts.iloc[1: , : ]
                    counter=0
                if int(k)==5 and counter==4:
                    counts = counts.iloc[1: , : ]
                    counter=0
                
    #basically without those hard coded forced loop, the algorithm runs out of groups to
    #assign rows with X to at lower values of k
        
    
#make new series of all groups left that need some help 
    fun= df.groupby("GroupNum").size().to_frame()
    needsMasked=fun[0]<=(int(k)-1)
    needs=fun[needsMasked]
    needs.to_csv("newsss.csv")
    fun.to_csv("news.csv")
    

    

    currentSize = 0
    groupNums = []
    count=0
    needs.reset_index(inplace= True)
    lastNum = 0
    secondToLast=0
#still some groups need assignments
    for i in range(len(needs)):
        groupNumber=needs['GroupNum'][i]
        size=needs[0][i]
        currentSize += size
        groupNums.append(groupNumber)

        if (i == len(needs)-1):
            
            if len(groupNums) > 0:
                    index = df.index[df['GroupNum'] == groupNums[0]]
                    df.at[index, 'GroupNum']= lastNum

        if currentSize >= int(k):
            for number in groupNums:
                indexList = df.index[df['GroupNum'] == number].tolist()
                for idx in indexList:
                    df.at[idx, 'GroupNum']= groupNums[0] #merge groups
                    lastNum = groupNums[0]
            groupNums = []
            currentSize = 0
        
 

    finalDF = df.sort_values(["GroupNum"])
    finalDF.reset_index(inplace=True)
    grouped= finalDF.groupby("GroupNum").size().to_frame()
   # print(grouped)
    secondToLast=len(grouped)-2
    countsMasked=grouped[0]<int(k)
    counts=grouped[countsMasked]
    #print(counts)
    numNeedsChange=0
    changeTo=0
    counts.reset_index(inplace= True)
    index=0
    if(counts.empty==False): #prevent index out of bounds
        lengthCounts=counts.size/2
        if(lengthCounts<1):
            lengthCounts=1
        while(index<lengthCounts): #theres some stragglers
            numNeedsChange=counts['GroupNum'][index]
            changeTo=grouped.index[secondToLast]
            #print("Error")
            
            finalDF.loc[finalDF.GroupNum == numNeedsChange, "GroupNum"] = changeTo
            index+=1

        grouped= finalDF.groupby("GroupNum").size().to_frame()
        secondToLast=len(grouped)-2
        merge=grouped.index[secondToLast]
        finalDF.loc[finalDF.GroupNum == changeTo, "GroupNum"] = merge
    





    anonymizedDF = finalDF.groupby("GroupNum").apply(anonymize)
    #comment out to test
    anonymizedDF.drop(columns = "GroupNum", inplace=True)
    anonymizedDF.drop(columns = "index", inplace=True)
    global preCount
    print("Precision Reduced: " + str(preCount) + " attributes")
    anonymizedDF.to_csv("kCensusData.csv")
    end = time.time()
    print(f"Runtime of the program is {end - start}")
    

    #uncomment to test, if counts is empty it worked
"""
    grouped= anonymizedDF.groupby("GroupNum").size().to_frame()
    countsMasked=grouped[0]<int(k)
    counts=grouped[countsMasked]
    print(counts)
"""


#anonymize based on column, if any are different in column, anonymize it
def anonymize(df):
    global preCount
    ageFLag = flagAttributes(df, "Age")
    OccupationFLag = flagAttributes(df, "Occupation")
    RaceFLag = flagAttributes(df, "Race")
    sexFlag = flagAttributes(df, "Sex")
    hoursFLag = flagAttributes(df, "HoursPerWeek")

    if sexFlag:
        df["Sex"] = "Male/Female"
        preCount +=1

    if ageFLag:
        toRound = 0
        for index, row in df.iterrows():
            toRound = df["Age"][index]
            break
        toRound = int(math.ceil(float(toRound) / 10.0)) * 10 #ceil to near 10
        df["Age"] = str(toRound)
        preCount+=1
    
    if hoursFLag:
        toRound = 0
        for index, row in df.iterrows():
            toRound = df["HoursPerWeek"][index]
            break
        toRound = int(math.ceil(float(toRound) / 10.0)) * 10 #ceil to near 10

        df["HoursPerWeek"] = str(toRound)
        preCount+=1
    
    if RaceFLag:
        df["Race"] =  "Human"
        preCount+=1
    
    if OccupationFLag:
        df["Occupation"] = "*"
        preCount+=1
    
    
    return df




def flagAttributes(df, attribute):
    list = []
    flag = False
    for index, row in df.iterrows():
        list.append(df[attribute][index])
    for i in range(len(list)):
        if list[i] != list[0]: #if any are different set flag to True
            flag = True


    return flag

        

if __name__ == "__main__":
    main()