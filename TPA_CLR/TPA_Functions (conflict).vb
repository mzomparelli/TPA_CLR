Imports System
Imports System.Data
Imports System.Data.SqlClient
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Collections.Generic
Imports VB = Microsoft.VisualBasic
Imports System.Globalization
Imports System.Text


#Region "Aggregate Functions"

<Serializable(), _
   SqlUserDefinedAggregate(Format.UserDefined, IsInvariantToNulls:=True, IsInvariantToDuplicates:=False, IsInvariantToOrder:=False, MaxByteSize:=8000)> _
<System.Runtime.InteropServices.ComVisible(False)> _
Public Structure CharDelimited
    Implements IBinarySerialize

    Dim DelimiterChar As String
    Dim LeftQualifierChar As String
    Dim RightQualifierChar As String
    Dim values As List(Of String)

    Public Sub Init()
        Me.values = New List(Of String)
        DelimiterChar = ","
    End Sub

    Public Sub Accumulate(ByVal value As SqlString, delimiter As String, LeftQualifier As String, RightQualifier As String)
        Me.LeftQualifierChar = LeftQualifier
        Me.RightQualifierChar = RightQualifier
        Me.DelimiterChar = delimiter
        Me.values.Add(value.ToString())
    End Sub

    Public Sub Merge(ByVal value As CharDelimited)
        Me.values.AddRange(value.values.ToArray())

    End Sub

    Public Function Terminate() As SqlString

        Dim d As String = ""
        Dim i As Integer = 1
        For Each s As String In Me.values
            If i = 1 Then
                'This is the first value.
                d = LeftQualifierChar & s & RightQualifierChar
            Else
                d = d & DelimiterChar & LeftQualifierChar & s & RightQualifierChar
            End If
            i += 1
        Next
        'Dim s As String = String.Join(DelimiterChar, Me.values.ToArray())


        Return New SqlString(d)



    End Function


    Public Sub Read(ByVal r As System.IO.BinaryReader) Implements Microsoft.SqlServer.Server.IBinarySerialize.Read

        Try
            DelimiterChar = r.ReadString()
            LeftQualifierChar = r.ReadString()
            RightQualifierChar = r.ReadString()
            Dim itemCount As Integer = r.ReadInt32()
            Me.values = New List(Of String)
            For i As Long = 0 To itemCount - 1 Step 1
                Me.values.Add(r.ReadString())
            Next
        Catch ex As Exception

        End Try

    End Sub

    Public Sub Write(ByVal w As System.IO.BinaryWriter) Implements Microsoft.SqlServer.Server.IBinarySerialize.Write

        Try
            w.Write(DelimiterChar)
            w.Write(LeftQualifierChar)
            w.Write(RightQualifierChar)
            w.Write(Me.values.Count)
            For Each s As String In Me.values
                w.Write(s)
            Next
        Catch ex As Exception

        End Try

    End Sub


End Structure

<Serializable(), SqlUserDefinedAggregate(Microsoft.SqlServer.Server.Format.UserDefined, _
                                         IsInvariantToDuplicates:=True, _
                                         IsInvariantToNulls:=False, _
                                         IsInvariantToOrder:=True, _
                                         IsNullIfEmpty:=True, _
                                         MaxByteSize:=-1, _
                                         Name:="Spread" _
                                         )> _
Public Structure Spread
    Implements IBinarySerialize

    Dim values As List(Of Value)
    Dim rawValues As List(Of String)
    Dim result As String
    Dim Count As Integer

    Dim HowMany As Integer
    Dim DisplayStyle As String

    Public Sub Init()
        Me.values = New List(Of Value)
        Me.rawValues = New List(Of String)
        result = String.Empty
        Count = -1
        HowMany = 1
    End Sub

    Public Sub Accumulate(ByVal value As SqlString, displayStyle As String)
        Me.HowMany = HowMany
        Me.DisplayStyle = displayStyle
        If value.IsNull Then
            Return
        End If
        Me.rawValues.Add(value.ToString())
    End Sub

    Public Sub Merge(ByVal group As Spread)
        Me.rawValues.AddRange(group.rawValues.ToArray())
    End Sub

    Public Function Terminate() As SqlString
        GetMostCommon()
        Dim e As Encoding = Encoding.Unicode
        Dim i As Integer = e.GetByteCount(result)
        'If i > 8000 Then
        '    Dim r() As String = result.Split(" : ".ToCharArray)
        '    Return New SqlString(r(0))
        'Else
        '    Return New SqlString(result)
        'End If

        If result.Length > 4000 Then
            Return New SqlString(Left(result, 3997) & "...")
        Else
            Return New SqlString(result)
        End If


    End Function

    Public Sub GetMostCommon()

        If HowMany < 1 Then
            HowMany = 1
        End If

        For Each s As String In rawValues
            AddOrIncrementValue(s)
        Next

        values.Sort()
        Dim x As Integer = 0

        Dim modeValue(values.Count - 1) As String
        Dim modeOccurences(values.Count - 1) As Integer

        For i As Integer = 0 To values.Count - 1 Step 1
            modeValue(i) = "N/A"
            modeOccurences(i) = 0
        Next

        For Each v As Value In values
            For z As Integer = 0 To values.Count - 1 Step 1
                If v.GroupOccurences = modeOccurences(z) Then
                    modeValue(z) = modeValue(z) & ", " & v.GroupValue
                    Exit For
                ElseIf v.GroupOccurences > modeOccurences(z) Then
                    For i As Integer = values.Count - 1 To z + 1 Step -1
                        modeValue(i) = modeValue(i - 1)
                        modeOccurences(i) = modeOccurences(i - 1)
                    Next

                    modeValue(z) = v.GroupValue
                    modeOccurences(z) = v.GroupOccurences
                    Exit For
                End If
            Next
        Next

        result = values.Count.ToString() & " : "
        For i As Integer = 0 To values.Count - 1 Step 1
            If modeValue(i) = "N/A" Or modeValue(i) = "" Then
                Exit For
            Else
                Select Case DisplayStyle
                    Case "Spread"
                        result = result & IIf(i > 0, ", ", "").ToString() & modeValue(i)
                    Case "Percent"
                        result = result & IIf(i > 0, ", ", "").ToString() & modeValue(i) & " : " & Decimal.Round((modeOccurences(i) / Convert.ToDecimal(rawValues.Count)) * 100, 2) & "%"
                    Case "Count"
                        result = result & IIf(i > 0, ", ", "").ToString() & modeValue(i) & " : " & modeOccurences(i)
                End Select
            End If
        Next

    End Sub

    Public Sub AddOrIncrementValue(ByVal value As SqlString)

        Dim found As Boolean = False

        For Each v As Value In values
            If v.GroupValue = value.ToString() Then
                v.GroupOccurences += 1
                found = True
                Exit For
            End If
        Next

        If Not found Then
            values.Add(New Value(value.ToString()))
        End If


    End Sub


    Public Sub Read(ByVal r As System.IO.BinaryReader) Implements Microsoft.SqlServer.Server.IBinarySerialize.Read

        Me.result = r.ReadString()
        Me.Count = r.ReadInt32()
        Dim iCount As Integer = r.ReadInt32()
        'HowMany = r.ReadInt32()
        DisplayStyle = r.ReadString()
        rawValues = New List(Of String)
        Me.values = New List(Of Value)

        For i As Integer = 0 To iCount - 1 Step 1
            Me.rawValues.Add(r.ReadString())
        Next



    End Sub

    Public Sub Write(ByVal w As System.IO.BinaryWriter) Implements Microsoft.SqlServer.Server.IBinarySerialize.Write

        w.Write(result)
        w.Write(Me.Count)
        w.Write(rawValues.Count)
        'w.Write(HowMany)
        w.Write(DisplayStyle)
        For Each s As String In rawValues
            w.Write(s)
        Next


    End Sub



End Structure

Public Class Value
    Implements IComparable

    Public GroupValue As String
    Public GroupOccurences As Integer

    Public Sub New(ByVal GroupValue As String)
        Me.GroupValue = GroupValue
        GroupOccurences = 1
    End Sub

    Public Function CompareTo(obj As Object) As Integer Implements System.IComparable.CompareTo
        Dim s As Value = CType(obj, Value)
        'Return String.Compare(Me.GroupValue, s.GroupValue)
        If Me.GroupValue < s.GroupValue Then
            Return -1
        ElseIf Me.GroupValue > s.GroupValue Then
            Return 1
        Else
            Return 0
        End If
    End Function
End Class


#End Region


Public Class T

#Region "Scaler-valued Functions"


#Region "Math Functions"

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Divide(<SqlFacet(Precision:=22, Scale:=10)> numerator As SqlDecimal, <SqlFacet(Precision:=22, Scale:=10)> denominator As SqlDecimal) As <SqlFacet(Precision:=22, Scale:=6)> SqlDecimal
        If denominator = 0 Then
            Return SqlDecimal.Null
        Else
            Return numerator / denominator
        End If

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function TLRate(<SqlFacet(Precision:=22, Scale:=10)> CPM As SqlDecimal, <SqlFacet(Precision:=22, Scale:=10)> MIN As SqlDecimal, <SqlFacet(Precision:=22, Scale:=10)> FLAT As SqlDecimal, <SqlFacet(Precision:=22, Scale:=10)> MILES As SqlDecimal) As <SqlFacet(Precision:=22, Scale:=6)> SqlDecimal

        Dim _CPM As Decimal
        Dim _MIN As Decimal
        Dim _FLAT As Decimal
        Dim _Miles As Decimal

        Try
            _CPM = CType(CPM, Decimal)
        Catch ex As Exception
            _CPM = Nothing
        End Try

        Try
            _MIN = CType(MIN, Decimal)
        Catch ex As Exception
            _MIN = Nothing
        End Try

        Try
            _FLAT = CType(FLAT, Decimal)
        Catch ex As Exception
            _FLAT = Nothing
        End Try

        Try
            _Miles = CType(MILES, Decimal)
        Catch ex As Exception
            _Miles = Nothing
        End Try

        Dim rate As Decimal

        If _CPM = 0 Or _CPM = Nothing Then
            If _FLAT = 0 Or _FLAT = Nothing Then
                rate = -1
                Return rate
            Else
                rate = _FLAT
                Return rate
            End If
        Else
            If _Miles = 0 Or _Miles = Nothing Then
                rate = -1
                Return rate
            Else
                rate = _CPM * _Miles
            End If
        End If

        If _MIN = 0 Or _MIN = Nothing Then
            Return rate
        ElseIf rate < MIN Then
            Return _MIN
        Else
            Return rate
        End If

    End Function



#End Region


#Region "String Functions"

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function ZipClean(Zip As String) As String

        If Zip.Length <= 3 Then
            'Bad data, cannot make assumption
            Return Zip
        ElseIf Zip.Length = 4 Then
            ' US Zip Code on the east coast missing the leading zero
            Return "0" & Zip
        ElseIf Zip.Length = 5 Then
            'Properly Spaced and formatted US and Canadian Zip Codes
            Return Zip
        ElseIf Zip.Length = 6 Then
            If IsNumeric(Zip.Substring(0, 1)) Then
                ' US Zip Code, Canadian ZIPs will start with a letter
                Return Zip.Substring(0, 5)
            Else
                Return Zip
            End If
        ElseIf Zip.Length = 7 Then
            'Candian zip with a random space in there ,space removed leaves it at 6
            Return Zip.Replace(" ", "")
        Else
            'Long form zip code and we just take the left 5
            Return Zip.Substring(0, 5)
        End If


    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Split(Text As String, delimiter As String, returnZeroBasedIndex As Integer) As String
        Dim s() As String = VB.Split(Text, delimiter)
        If returnZeroBasedIndex <= s.Length - 1 Then
            Return s(returnZeroBasedIndex)
        Else
            Return ""
        End If
    End Function

#End Region


#Region "Date Functions"

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function WeekNum(d As Date) As Integer
        Return DatePart(DateInterval.WeekOfYear, d)
    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function YearWeek(d As Date) As String
        Dim w As Integer = DatePart(DateInterval.WeekOfYear, d)
        Dim y As Integer = Year(d)

        Dim weekyear As String = CType(IIf(w > 9, y & "-" & w, y & "-" & "0" & w), String)

        Return weekyear
    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function YearMonth(d As Date) As String
        Dim m As Integer = Month(d)
        Dim y As Integer = Year(d)

        Dim monthyear As String = CType(IIf(m > 9, y & "-" & m, y & "-" & "0" & m), String)

        Return monthyear
    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function WeekEndingDate(d As Date) As Date
        Select Case Weekday(d)
            Case 1
                Return d.AddDays(6)
            Case 2
                Return d.AddDays(5)
            Case 3
                Return d.AddDays(4)
            Case 4
                Return d.AddDays(3)
            Case 5
                Return d.AddDays(2)
            Case 6
                Return d.AddDays(1)
            Case 7
                Return d
        End Select
    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function WeekStartingDate(d As Date) As Date

        Select Case Weekday(d)
            Case 1
                Return d
            Case 2
                Return d.AddDays(-1)
            Case 3
                Return d.AddDays(-2)
            Case 4
                Return d.AddDays(-3)
            Case 5
                Return d.AddDays(-4)
            Case 6
                Return d.AddDays(-5)
            Case 7
                Return d.AddDays(-6)
        End Select
    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function MonthNumber(month As String) As Integer

        Select Case month
            Case "January", "Jan"
                Return 1
            Case "February", "Feb"
                Return 2
            Case "March", "Mar"
                Return 3
            Case "April", "Apr"
                Return 4
            Case "May"
                Return 5
            Case "June", "Jun"
                Return 6
            Case "July", "Jul"
                Return 7
            Case "August", "Aug"
                Return 8
            Case "September", "Sep"
                Return 9
            Case "October", "Oct"
                Return 10
            Case "November", "Nov"
                Return 11
            Case "December", "Dec"
                Return 12
            Case Else
                Return 0
        End Select


    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function MonthName(iMonth As Integer) As String

        Select Case iMonth
            Case 1
                Return "January"
            Case 2
                Return "February"
            Case 3
                Return "March"
            Case 4
                Return "April"
            Case 5
                Return "May"
            Case 6
                Return "June"
            Case 7
                Return "July"
            Case 8
                Return "August"
            Case 9
                Return "September"
            Case 10
                Return "October"
            Case 11
                Return "November"
            Case 12
                Return "December"
            Case Else
                Return "n/a"
        End Select


    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function MonthNameShort(iMonth As Integer) As String

        Select Case iMonth
            Case 1
                Return "Jan"
            Case 2
                Return "Feb"
            Case 3
                Return "Mar"
            Case 4
                Return "Apr"
            Case 5
                Return "May"
            Case 6
                Return "Jun"
            Case 7
                Return "Jul"
            Case 8
                Return "Aug"
            Case 9
                Return "Sep"
            Case 10
                Return "Oct"
            Case 11
                Return "Nov"
            Case 12
                Return "Dec"
            Case Else
                Return "n/a"
        End Select


    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_EndDate(numberToAdd As Integer, WeekMonthYear As String) As Date
        Select Case WeekMonthYear.ToLower()
            Case "day", "d"
                Return Date_DayEndDate(numberToAdd)
            Case "week", "w"
                Return Date_WeekEndDate(numberToAdd)
            Case "month", "m"
                Return Date_MonthEndDate(numberToAdd)
            Case "year", "y"
                Return Date_YearEndDate(numberToAdd)
            Case Else
                Return Nothing
        End Select
    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_StartDate(numberToAdd As Integer, WeekMonthYear As String) As Date
        Select Case WeekMonthYear.ToLower()
            Case "day", "d"
                Return Date_DayStartDate(numberToAdd)
            Case "week", "w"
                Return Date_WeekStartDate(numberToAdd)
            Case "month", "m"
                Return Date_MonthStartDate(numberToAdd)
            Case "year", "y"
                Return Date_YearStartDate(numberToAdd)
            Case Else
                Return Nothing
        End Select
    End Function

    Private Shared Function Date_DayEndDate(numberToAdd As Integer) As Date
        Dim d As Date = Convert.ToDateTime(Now.Month & "/" & Now.Day & "/" & Now.Year & " 11:59:59 PM")
        Return d.AddDays(numberToAdd)
    End Function

    Private Shared Function Date_DayStartDate(numberToAdd As Integer) As Date
        Dim d As Date = Convert.ToDateTime(Now.Month & "/" & Now.Day & "/" & Now.Year & " 12:00:00 AM")
        Return d.AddDays(numberToAdd)
    End Function

    Private Shared Function Date_WeekEndDate(numberToAdd As Integer) As Date
        Dim d As Date = Convert.ToDateTime(Now.Month & "/" & Now.Day & "/" & Now.Year & " 11:59:59 PM")

        Dim iDays As Integer = 0

        Select Case d.DayOfWeek
            Case DayOfWeek.Sunday
                iDays = 6
            Case DayOfWeek.Monday
                iDays = 5
            Case DayOfWeek.Tuesday
                iDays = 4
            Case DayOfWeek.Wednesday
                iDays = 3
            Case DayOfWeek.Thursday
                iDays = 2
            Case DayOfWeek.Friday
                iDays = 1
            Case DayOfWeek.Saturday
                iDays = 0
        End Select


        If numberToAdd = 0 Then
            'do nothing to iDays
        ElseIf numberToAdd > 0 Then
            For i As Integer = 1 To numberToAdd Step 1
                iDays += 7
            Next
        Else
            For i As Integer = 1 To numberToAdd * -1 Step 1
                iDays -= 7
            Next
        End If


        Return d.AddDays(iDays)
    End Function

    Private Shared Function Date_WeekStartDate(numberToAdd As Integer) As Date
        Dim d As Date = Convert.ToDateTime(Now.Month & "/" & Now.Day & "/" & Now.Year & " 12:00:00 AM")

        Dim iDays As Integer = 0

        Select Case d.DayOfWeek
            Case DayOfWeek.Sunday
                iDays = 0
            Case DayOfWeek.Monday
                iDays = -1
            Case DayOfWeek.Tuesday
                iDays = -2
            Case DayOfWeek.Wednesday
                iDays = -3
            Case DayOfWeek.Thursday
                iDays = -4
            Case DayOfWeek.Friday
                iDays = -5
            Case DayOfWeek.Saturday
                iDays = -6
        End Select

        If numberToAdd = 0 Then
            'do nothing to iDays
        ElseIf numberToAdd > 0 Then
            For i As Integer = 1 To numberToAdd Step 1
                iDays += 7
            Next
        Else
            For i As Integer = 1 To numberToAdd * -1 Step 1
                iDays -= 7
            Next
        End If


        Return d.AddDays(iDays)


    End Function

    Private Shared Function Date_MonthEndDate(numberToAdd As Integer) As Date
        Dim m As Integer = Now.Month
        Dim y As Integer = Now.Year

        If numberToAdd = 0 Then
            'Do nothing to m and y
        ElseIf numberToAdd > 0 Then
            For i As Integer = 1 To numberToAdd Step 1
                If m = 12 Then
                    m = 1
                    y += 1
                Else
                    m += 1
                End If
            Next
        Else
            For i As Integer = 1 To numberToAdd * -1 Step 1
                If m = 1 Then
                    m = 12
                    y -= 1
                Else
                    m -= 1
                End If
            Next
        End If

        Dim iDays As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime(m & "/" & iDays & "/" & y & " 11:59:59 PM")
    End Function

    Private Shared Function Date_MonthStartDate(numberToAdd As Integer) As Date
        Dim m As Integer = Now.Month
        Dim y As Integer = Now.Year

        If numberToAdd = 0 Then
            'Do nothing to m and y
        ElseIf numberToAdd > 0 Then
            For i As Integer = 1 To numberToAdd Step 1
                If m = 12 Then
                    m = 1
                    y += 1
                Else
                    m += 1
                End If
            Next
        Else
            For i As Integer = 1 To numberToAdd * -1 Step 1
                If m = 1 Then
                    m = 12
                    y -= 1
                Else
                    m -= 1
                End If
            Next
        End If

        'Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime(m & "/1/" & y & " 12:00:00 AM")
    End Function

    Private Shared Function Date_YearEndDate(numberToAdd As Integer) As Date
        Dim m As Integer = Now.Month
        Dim y As Integer = Now.Year

        If numberToAdd = 0 Then
            'Do nothing to m and y
        ElseIf numberToAdd > 0 Then
            y += numberToAdd
        Else
            y -= numberToAdd * -1
        End If

        'Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime("12/31/" & y & " 11:59:59 PM")
    End Function

    Private Shared Function Date_YearStartDate(numberToAdd As Integer) As Date
        Dim m As Integer = Now.Month
        Dim y As Integer = Now.Year

        If numberToAdd = 0 Then
            'Do nothing to m and y
        ElseIf numberToAdd > 0 Then
            y += numberToAdd
        Else
            y -= numberToAdd * -1
        End If

        'Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime("1/1/" & y & " 12:00:00 AM")
    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_12MonthRollingStartDate() As Date

        Dim d As Date = Date.Now.AddMonths(-12)

        'Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime(d.Month & "/1/" & d.Year & " 12:00:00 AM")

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_12MonthRollingEndDate() As Date

        Dim d As Date = Date.Now.AddMonths(-1)

        'Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime(d.Month & "/" & Date.DaysInMonth(d.Year, d.Month) & "/" & d.Year & " 11:59:59 PM")

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function IntegerToDate(dateNumber As Integer, currentFormat As String) As String

        Dim s As String = dateNumber.ToString()
        Dim d As DateTime = Nothing
        DateTime.TryParseExact(s, currentFormat, CultureInfo.CreateSpecificCulture("en-US"), DateTimeStyles.None, d)
        If d = Nothing Then
            Return ""
        Else
            Return d.Month & "/" & d.Day & "/" & d.Year
        End If

    End Function

#Region "Depracated"



    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_PreviousWeekStartDate() As Date
        Dim d As Date = Convert.ToDateTime(Now.Month & "/" & Now.Day & "/" & Now.Year & " 12:00:00 AM")
        Select Case d.DayOfWeek

            Case DayOfWeek.Sunday
                Return d.AddDays(-7)
            Case DayOfWeek.Monday
                Return d.AddDays(-8)
            Case DayOfWeek.Tuesday
                Return d.AddDays(-9)
            Case DayOfWeek.Wednesday
                Return d.AddDays(-10)
            Case DayOfWeek.Thursday
                Return d.AddDays(-11)
            Case DayOfWeek.Friday
                Return d.AddDays(-12)
            Case DayOfWeek.Saturday
                Return d.AddDays(-13)
        End Select

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_PreviousWeekEndDate() As Date
        Dim d As Date = Convert.ToDateTime(Now.Month & "/" & Now.Day & "/" & Now.Year & " 11:59:59 PM")
        Select Case d.DayOfWeek

            Case DayOfWeek.Sunday
                Return d.AddDays(-1)
            Case DayOfWeek.Monday
                Return d.AddDays(-2)
            Case DayOfWeek.Tuesday
                Return d.AddDays(-3)
            Case DayOfWeek.Wednesday
                Return d.AddDays(-4)
            Case DayOfWeek.Thursday
                Return d.AddDays(-5)
            Case DayOfWeek.Friday
                Return d.AddDays(-6)
            Case DayOfWeek.Saturday
                Return d.AddDays(-7)
        End Select

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_CurrentWeekStartDate() As Date
        Dim d As Date = Convert.ToDateTime(Now.Month & "/" & Now.Day & "/" & Now.Year & " 12:00:00 AM")
        Select Case d.DayOfWeek

            Case DayOfWeek.Sunday
                Return d
            Case DayOfWeek.Monday
                Return d.AddDays(-1)
            Case DayOfWeek.Tuesday
                Return d.AddDays(-2)
            Case DayOfWeek.Wednesday
                Return d.AddDays(-3)
            Case DayOfWeek.Thursday
                Return d.AddDays(-4)
            Case DayOfWeek.Friday
                Return d.AddDays(-5)
            Case DayOfWeek.Saturday
                Return d.AddDays(-6)
        End Select

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_CurrentWeekEndDate() As Date
        Dim d As Date = Convert.ToDateTime(Now.Month & "/" & Now.Day & "/" & Now.Year & " 11:59:59 PM")
        Select Case d.DayOfWeek

            Case DayOfWeek.Sunday
                Return d.AddDays(6)
            Case DayOfWeek.Monday
                Return d.AddDays(5)
            Case DayOfWeek.Tuesday
                Return d.AddDays(4)
            Case DayOfWeek.Wednesday
                Return d.AddDays(3)
            Case DayOfWeek.Thursday
                Return d.AddDays(2)
            Case DayOfWeek.Friday
                Return d.AddDays(1)
            Case DayOfWeek.Saturday
                Return d
        End Select

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_PreviousMonthStartDate() As Date
        Dim m As Integer = Now.Month
        Dim y As Integer = Now.Year
        If m = 1 Then
            m = 12
            y -= 1
        Else
            m -= 1
        End If
        'Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime(m & "/1/" & y & " 12:00:00 AM")

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_PreviousMonthEndDate() As Date
        Dim m As Integer = Now.Month
        Dim y As Integer = Now.Year
        If m = 1 Then
            m = 12
            y -= 1
        Else
            m -= 1
        End If
        Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime(m & "/" & i & "/" & y & " 11:59:59 PM")

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_CurrentMonthStartDate() As Date
        Dim m As Integer = Now.Month
        Dim y As Integer = Now.Year
        If m = 1 Then
            m = 12
            y -= 1
        Else
            m -= 1
        End If
        'Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime(Now.Month & "/1/" & Now.Year & " 12:00:00 AM")

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)> _
    Public Shared Function Date_CurrentMonthEndDate() As Date
        Dim m As Integer = Now.Month
        Dim y As Integer = Now.Year
        Dim i As Integer = Date.DaysInMonth(y, m)
        Return Convert.ToDateTime(m & "/" & i & "/" & y & " 11:59:59 PM")

    End Function

#End Region





#End Region





#End Region

End Class
