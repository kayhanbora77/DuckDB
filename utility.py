#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  3 20:03:32 2025

@author: kayhan
"""
import copy

import pandas as pd
import sqlDB

def addDepartureDateToList(insertDepartureList, departureList, ind):
    insertDepartureList.append(departureList[ind+1])
    insertDepartureList.append(departureList[ind+2])


def addFlightNumberToList(insertFlightNumberList, flightNumberList, ind):
    insertFlightNumberList.append(flightNumberList[ind+1])
    insertFlightNumberList.append(flightNumberList[ind+2])


def getHours(firstDate, nextDate):
    diff = nextDate-firstDate
    hours = diff.total_seconds()/3600
    return hours

def insertNewRow(row, groups):   
    if len(groups) == 0 or len(groups) == 1 :
        print("DO NOTHING")
        return
    insertRow = copy.copy(row)
    updateRow = copy.copy(row)
    i=1
    for grp in groups:    
        print("GROUPS==>"+str(i), grp)
        j=1
        for element in grp:
            insertRow['FlightNumber'+str(i+j-1)] = row['FlightNumber'+str(i+j+1)]
            insertRow['DepartureDateLocal'+str(i+j-1)] = row['DepartureDateLocal'+str(i+j+1)]
            insertRow['FlightNumber'+str(i+j+1)]=None
            insertRow['DepartureDateLocal'+str(i+j+1)]=None 
            
            updateRow['FlightNumber'+str(i+j+1)]=None
            updateRow['DepartureDateLocal'+str(i+j+1)]=None            
            #print("ELEMENT==>"+str(j), element)
            j=j+1        
        i=i+1
        
    #print("INSERT_ROW===>", insertRow)
    for n in range(len(insertRow)):        
        if (insertRow[n])=="NULL":
            insertRow[n]=None
            updateRow[n]=None
        print(n, insertRow[n])
        
    sqlDB.insertTable_TBO3_2021_100_UPDATE(insertRow)    
    #print("UPDATE_ROW===>", updateRow)
    sqlDB.updateTable_TBO3_2021_100(updateRow)
    return

def groupByDepartureDateAndFlightNumber(row, departureDateList, flightNumberList):
    groups = []
    # first=pd.to_datetime(
    #     departureDateList[0], format="%Y-%m-%d %H:%M:%S.%f")
    first = departureDateList[0]
    current_group = [first]

    for prev, curr in zip(departureDateList, departureDateList[1:]):
        prevDate = pd.to_datetime(
            prev, format="%Y-%m-%d %H:%M:%S.%f")
        currDate = pd.to_datetime(
            curr, format="%Y-%m-%d %H:%M:%S.%f")
        diff_hours = (currDate - prevDate).total_seconds() / 3600

        if (diff_hours < 24):
            current_group.append(curr)
            #insertRow = getInsertRow(row, flightNumberList, departureDateList)
            #updateRow = getUpdateRow(row, flightNumberList, departureDateList)
            #sqlDB.insertTable_TBO3_2021_100_UPDATE(insertRow)
            #sqlDB.updateTable_TBO3_2021_100_UPDATE(updateRow)
            
        else:
            groups.append(current_group)
            current_group = [curr]

    groups.append(current_group)
    return groups
    
def groupByFlightNumber(flightNumberList, departureDateGroups):    
    return    
    
    

def getDepartureDateAndFlightList(row):
    departureDateList = []
    flightNumberList = []

    for i in range(1, 7):
        flightNumber = row['FlightNumber'+str(i)]
        departureDate = row['DepartureDateLocal'+str(i)]

        if pd.notnull(flightNumber) and flightNumber != 'NULL':
            flightNumberList.append(flightNumber)

        if pd.notnull(departureDate) and departureDate != 'NULL':
            departureDateList.append(departureDate)
     
    return departureDateList, flightNumberList

def getUpdateRow():
    return
