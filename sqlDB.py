#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  3 16:25:14 2025

@author: kayhan
"""

import duckdb
import pandas as pd
# from sqlalchemy import create_engine, text

dbname = "/home/kayhan/my_database/my_db.duckdb"
# dbName= "/home/kayhan/my_database/"
# engine = create_engine("duckdb:///"+dbname)


def createTable_100Rows():
    conn = getConnection()

    closeConnection(conn)


def insertTable_TBO3_2021_100_UPDATE(insertRow):
    conn = getConnection()
    insertStmt = """
    INSERT INTO my_db.main.TBO3_2021_100_UPDATE(PaxName, BookingRef, ETicketNo, 
    ClientCode, Airline, JourneyType, FlightNumber1, FlightNumber2,
    FlightNumber3, FlightNumber4, FlightNumber5, FlightNumber6,
    FlightNumber7, DepartureDateLocal1, DepartureDateLocal2,
    DepartureDateLocal3, DepartureDateLocal4,
    DepartureDateLocal5, DepartureDateLocal6, DepartureDateLocal7,
    Airport1, Airport2, Airport3, Airport4, Airport5, Airport6, Airport7, Airport8)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)    """

    params = (insertRow[1], insertRow[2], "INSERT", insertRow[4],
              insertRow[5], insertRow[6], insertRow[7], insertRow[8],
              insertRow[9], insertRow[10], insertRow[11], insertRow[12],
              insertRow[13], insertRow[14], insertRow[15], insertRow[16],
              insertRow[17], insertRow[18], insertRow[19], insertRow[20],
              insertRow[21], insertRow[22], insertRow[23], insertRow[24],
              insertRow[25], insertRow[26], insertRow[27], insertRow[28])    
    conn.execute(insertStmt, params)
    closeConnection(conn)


def updateTable_TBO3_2021_100(updateRow):
    conn = getConnection()

    updateStmt = """
    UPDATE my_db.main.TBO3_2021_100
    SET ETicketNo=?, FlightNumber1 = ?, FlightNumber2 = ?, FlightNumber3 = ?,
    FlightNumber4 = ?, FlightNumber5 = ?, FlightNumber6 = ?,
    FlightNumber7 = ?, DepartureDateLocal1 = ?, DepartureDateLocal2 = ?,
    DepartureDateLocal3 = ?, DepartureDateLocal4 = ?, DepartureDateLocal5 = ?,
    DepartureDateLocal6 = ?, DepartureDateLocal7 = ?
    WHERE PaxName=? AND BookingRef=?
    """
    params = ("UPDATED", updateRow[7], updateRow[8], updateRow[9], updateRow[10],
              updateRow[11], updateRow[12], updateRow[13], updateRow[14],
              updateRow[15], updateRow[16], updateRow[17], updateRow[18],
              updateRow[19], updateRow[20],
              updateRow[1], updateRow[2])

    conn.execute(updateStmt, params)
    closeConnection(conn)


def getAllDataFrom_TBO3_2021_100():
    conn = getConnection()
    query = """
        SELECT * FROM my_db.main.TBO3_2021_100
        WHERE JourneyType <>?
    """
    result = pd.read_sql(query, conn, params=["Return"])
    closeConnection(conn)
    return result


def getDataFrom_TBO3_2021_100():
    conn = getConnection()
    query = """
        SELECT * FROM my_db.main.TBO3_2021_100
        WHERE PaxName=? AND BookingRef=?
        """
    result = pd.read_sql(query, conn,
                         params=["FARJANA SIRAJUDDIN GODER",
                                 "31947988"])
    closeConnection(conn)
    return result


def getConnection():
    conn = duckdb.connect(database=dbname, read_only=False)
    return conn


def closeConnection(conn):
    conn.close()
