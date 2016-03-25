Imports System.ComponentModel
Imports System.Data.SqlClient

Public Class SQLConn
    Implements IDisposable

#Region "Declarations"
    Private zFilename As String = "DatabaseConnections.dll version 1.0.0.0"
    Private zClass As String = "SQLServerConnection"


    Private strConnectionString As String
    Private strUserPassword As String
    Private CommandBuilder As SqlClient.SqlCommandBuilder
    Private dt As New DataTable
    Private da As SqlClient.SqlDataAdapter
    Private db As New SqlClient.SqlConnection

    'Private _conn As New SqlConnection
    'Public cmd As New SqlCommand


#End Region

#Region "Properties and Structures"

    <System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)> _
    Public Overrides Function GetHashCode() As Integer
        Return MyBase.GetHashCode()
    End Function
    <System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)> _
    Public Overrides Function ToString() As String
        Return "Michael Zomparelli"
    End Function
    <System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)> _
    Public Overrides Function Equals(ByVal obj As Object) As Boolean
        Return Object.Equals(Me, obj)
    End Function

    Private strUserID As String
    Public ReadOnly Property UserID() As String
        Get
            Return strUserID
        End Get
    End Property

    Private strSQLServer As String
    Public ReadOnly Property SQLServer() As String
        Get
            Return strSQLServer
        End Get
    End Property

    Private strSQLString As String
    Public ReadOnly Property SQLString() As String
        Get
            Return strSQLString
        End Get
    End Property

    Private strDatabase As String
    Public ReadOnly Property Database() As String
        Get
            Return strDatabase
        End Get
    End Property

    Public ReadOnly Property ConnectionString() As String
        Get
            Return strConnectionString
        End Get
    End Property

    Public QueryResults As New StructureFillTable
    <System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)> _
    Public Structure StructureFillTable

        Public Overrides Function Equals(ByVal obj As Object) As Boolean
            Return Object.Equals(Me, obj)
        End Function

        Public Table As DataTable
        Public DataAdapter As SqlClient.SqlDataAdapter

        Public Rows As Integer
        Public Errors As String
    End Structure

    Private structureSQL As New SQLStructure

    Public Property SQL() As SQLStructure
        Get
            Return structureSQL
        End Get
        Set(ByVal value As SQLStructure)
            structureSQL = value
        End Set
    End Property


    <System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)> _
    Public Structure SQLStructure

        Public Overrides Function Equals(ByVal obj As Object) As Boolean
            Return Object.Equals(Me, obj)
        End Function

        Public SelectField() As String
        Public Table() As String
        Public WhereClause() As String
    End Structure

    Public ReadOnly Property TableChanges() As DataTable
        Get
            Return dt.GetChanges
        End Get
    End Property


    Private _UseWindowsCredentials As Boolean
    Public Property UseWindowsCredentials() As Boolean
        Get
            Return _UseWindowsCredentials
        End Get
        Set(ByVal value As Boolean)
            _UseWindowsCredentials = value
            If value = True Then
                db.ConnectionString = "Provider=SQLOLEDB;Data Source=" & strSQLServer & ";Integrated Security=True;Pooling=False;Initial Catalog=" & strDatabase & ";"

            Else
                MakeConnectionString()
            End If
        End Set
    End Property

#End Region

#Region "Public Methods"

    Public Sub New(ByVal Server As String, ByVal UserID As String, ByVal UserPassword As String, ByVal Database As String)


        strDatabase = Database
        strSQLServer = Server
        strUserPassword = UserPassword
        strUserID = UserID
        MakeConnectionString()
        ReDim structureSQL.Table(0)
        ReDim structureSQL.SelectField(99)
        ReDim structureSQL.WhereClause(99)
    End Sub


    <Description("Uses the specified SQL string to run a query")> _
    Public Overloads Sub RunQuery(ByVal SQL As String)
        strSQLString = SQL
        ClearQueryResults()
        QueryResults = FillTable()
    End Sub
    <Description("Uses the specified SQL Structure to run a query. A SQL string will be created from the structure.")> _
    Public Overloads Sub RunQuery(ByVal SQL As SQLStructure)
        If MakeSQLString(SQL) Then
            RunQuery(strSQLString)
        Else
            MsgBox("There is an error in the SQL Structure.", MsgBoxStyle.Exclamation, "zControls")
        End If

    End Sub
    <Description("Uses the classes SQL structure to build the SQL string.")> _
    Public Overloads Sub RunQuery()
        RunQuery(structureSQL)
    End Sub

    Public Function ExecuteNonQuery(ByVal sql As String) As String
        Dim rows As String = ""
        Try
            db.Open()
            Dim cmd As SqlCommand = New SqlCommand(sql, db)
            rows = cmd.ExecuteNonQuery().ToString()
        Catch ex As Exception
            rows = ex.Message
        Finally
            db.Close()
        End Try

        Return rows

    End Function

    ''' <summary>
    ''' This procedure calls UpdateDatabase() if SaveChanges is TRUE then re-runs the query to ensure updated data.
    ''' </summary>
    ''' <param name="SaveChanges">If TRUE UpdateDatabase() is called</param>
    ''' <remarks></remarks>
    Public Sub RefreshData(ByVal SaveChanges As Boolean)
        If SaveChanges Then UpdateDatabase()
        RunQuery(strSQLString)
    End Sub

    ''' <summary>
    ''' This procedure will only update tables that have keys.
    ''' </summary>
    ''' <remarks></remarks>
    Public Sub UpdateDatabase()
        Call SetSQLCommands()
        Try
            QueryResults.DataAdapter.Update(Me.TableChanges)
            QueryResults.Table.AcceptChanges()
        Catch exNull As ArgumentNullException
            'this means no changes have been made
        Catch ex As Exception
            MsgBox("File: " & Me.zFilename & Environment.NewLine & "Class: " & Me.zClass & Environment.NewLine & "Method: UpdateDatabase()" & Environment.NewLine & Environment.NewLine & ex.Message & Environment.NewLine & Environment.NewLine & "This error may prevent the data from being updated in the database.", MsgBoxStyle.Critical, "Michael Zomparelli")
        Finally

        End Try
    End Sub

#End Region

#Region "Private Methods"

    Private Function MakeSQLString(ByVal SQL As SQLStructure) As Boolean
        Dim strSQL As String = ""

        Dim a As String = ""
        Dim fCount As Integer, wCount As Integer
        Try
            fCount = SQL.SelectField.Length - 1
            a = "SELECT "
            For i As Integer = 0 To fCount Step 1
                If Not SQL.SelectField(i) = "" And SQL.SelectField(i + 1) = "" Then
                    If SQL.SelectField(i) = "*" Then
                        a = a & "* FROM " & SQL.Table(0) & " "
                        Exit For
                    Else
                        a = a & "[" & SQL.SelectField(i) & "] FROM " & SQL.Table(0) & " "
                    End If
                    Exit For
                ElseIf Not SQL.SelectField(i) = "" And Not SQL.SelectField(i + 1) = "" Then
                    a = a & "[" & SQL.SelectField(i) & "], "
                Else
                    Exit For
                End If
            Next
            wCount = SQL.WhereClause.Length - 1
            a = a & "WHERE ("
            For i As Integer = 0 To wCount Step 1
                If Not SQL.WhereClause(i) = "" And SQL.WhereClause(i + 1) = "" Then
                    a = a & SQL.WhereClause(i) & ")"
                    Exit For
                ElseIf Not SQL.WhereClause(i) = "" Then
                    a = a & SQL.WhereClause(i) & ") OR "
                Else
                    Exit For
                End If
            Next
            strSQLString = a
            Return True
        Catch ex As Exception
            MsgBox(ex.Message)
            Return False
        End Try
    End Function

    Private Sub SetSQLCommands()

        If Not QueryResults.DataAdapter.InsertCommand Is Nothing Then
            QueryResults.DataAdapter.InsertCommand = Nothing
        End If
        If Not QueryResults.DataAdapter.UpdateCommand Is Nothing Then
            QueryResults.DataAdapter.UpdateCommand = Nothing
        End If
        If Not QueryResults.DataAdapter.DeleteCommand Is Nothing Then
            QueryResults.DataAdapter.DeleteCommand = Nothing
        End If

        CommandBuilder.SetAllValues = False
        'This only works if the datatable has a key
        QueryResults.DataAdapter.InsertCommand = CommandBuilder.GetInsertCommand
        QueryResults.DataAdapter.UpdateCommand = CommandBuilder.GetUpdateCommand
        QueryResults.DataAdapter.DeleteCommand = CommandBuilder.GetDeleteCommand

    End Sub

    Private Sub MakeConnectionString()
        'strConnectionString = "Provider=SQLOLEDB;Data Source=" & strSQLServer & ";Persist Security Info=True;User ID=" & strUserID & ";Password=" & strUserPassword & ";Catalog=" & strDatabase & ";"
        strConnectionString = String.Format("Data Source={0};Initial Catalog={1};User id={2};Password={3}", Me.strSQLServer, Me.strDatabase, Me.strUserID, Me.strUserPassword)
        db.ConnectionString = strConnectionString
        '_conn.ConnectionString = strConnectionString
        'cmd.CommandType = CommandType.StoredProcedure
    End Sub

    Private Sub ClearQueryResults()
        Try
            Me.QueryResults.Table.Clear()
            Me.QueryResults.Rows = 0
            Me.QueryResults.Errors = ""
        Catch ex As Exception

        End Try

    End Sub

    Private Function FillTable() As StructureFillTable
        Dim StructureTable As New StructureFillTable

        Try
            db.Open()
            da = New SqlClient.SqlDataAdapter(strSQLString, db)
            da.Fill(dt)
            StructureTable.DataAdapter = da
            StructureTable.Table = dt
            StructureTable.Rows = dt.Rows.Count
            StructureTable.Errors = ""
            CommandBuilder = New SqlClient.SqlCommandBuilder(da)
            Return StructureTable
            StructureTable = Nothing
        Catch ex As Exception
            'All exceptions are stored in a variable without displaying a message box to the user.
            'This allows the error to be deciphered so it can be determined in code if a message box is necessary.
            'This will be determined outside of this function.

            StructureTable.Errors = ex.Message
            Return StructureTable
            StructureTable = Nothing
        Finally
            db.Close()
        End Try
    End Function

#End Region

    

    Private disposedValue As Boolean = False        ' To detect redundant calls

    ' IDisposable
    Protected Overridable Sub Dispose(ByVal disposing As Boolean)
        If Not Me.disposedValue Then
            If disposing Then
                ' TODO: free other state (managed objects).
            End If

            ' TODO: free your own state (unmanaged objects).
            ' TODO: set large fields to null.
        End If
        Me.disposedValue = True
    End Sub

#Region " IDisposable Support "
    ' This code added by Visual Basic to correctly implement the disposable pattern.
    Public Sub Dispose() Implements IDisposable.Dispose
        ' Do not change this code.  Put cleanup code in Dispose(ByVal disposing As Boolean) above.
        Dispose(True)
        GC.SuppressFinalize(Me)
    End Sub
#End Region

End Class



