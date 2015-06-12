namespace LogAlyzer

open System
open System.Collections.Generic
open System.Data
open System.IO

type ILineReader =
    inherit IDisposable
    abstract EndOfStream : bool
    abstract Close : unit -> unit
    abstract ReadLine : unit -> string

type IisLogReader(input:ILineReader, filter:IDataRecord->bool) =
    let fields = Dictionary()
    let customFields = List()
    let keys = List()
    let mutable current = Array.empty
    let mutable custom = lazy Array.empty<Lazy<obj>>

    let addField x = 
        let i = keys.Count
        keys.Add(x)
        fields.Add(x, i)

    let ordinalOf name = fields.[name]
    let isHeaderLine (line:string) = line.[0] = '#' 
    let get index = 
        if index < current.Length then
            current.[index] :> obj
        else custom.Force().[index - current.Length].Force()
    let getString = string << get

    new(path, filter) = 
        let file = new StreamReader(new BufferedStream(File.OpenRead(path)))
        new IisLogReader({ new ILineReader with
                member this.EndOfStream = file.EndOfStream
                member this.Close() = file.Close()
                member this.ReadLine() = file.ReadLine()
                member this.Dispose() = file.Dispose() 
        }, filter);

    member this.ReadHeaders() = 
        let line = input.ReadLine()
        if not <| isHeaderLine line then
            raise(InvalidOperationException())
        else
            if line.StartsWith("#Fields:") then
                fields.Clear()
                keys.Clear()
                let parts = line.Split([|' '|], StringSplitOptions.RemoveEmptyEntries)
                for i = 1 to parts.Length - 1 do
                    addField parts.[i]
                for (key, _) in customFields do
                    addField key
                current <- Array.zeroCreate(parts.Length - 1)
            else
                this.ReadHeaders()

    member this.Item with get index = get index  

    member this.Item with get name = this.[ordinalOf name]

    member this.AddCustomField item = customFields.Add item

    member this.Read() =
        let rec next() =
            match input.ReadLine() with
            | null -> false
            | line -> 
                if isHeaderLine line then
                    this.ReadHeaders()
                    next()
                else
                    let values = line.Split([|' '|])
                    if values.Length <> current.Length then 
                        false
                    else
                        current <- values
                        custom <- lazy (customFields |> Seq.map (fun x -> lazy (snd x) this) |> Seq.toArray)
                        if filter(this) then
                            true
                        else next()
        next()
    
    member private this.NotSupported() = raise(NotSupportedException())

    interface IDataReader with

        member this.Depth = 0
        member this.IsClosed = input.EndOfStream
        member this.RecordsAffected = 0
        member this.Close() = input.Close()
        member this.NextResult() = false
        member this.Dispose() = input.Dispose()
        member this.FieldCount = fields.Count
        member this.Item with get (index:int) = this.[index]
        member this.Item with get (name:string) = this.[name]
        member this.GetName i = keys.[i]
        member this.GetDataTypeName i = typeof<string>.Name
        member this.GetFieldType i = typeof<string>
        member this.GetValue i = get i
        member this.GetValues values = 
            let len = Math.Max(values.Length, current.Length)
            for i = 0 to len - 1 do
                values.[i] <- this.[i]
            len
        member this.GetOrdinal name = ordinalOf name
        member this.GetBoolean i = bool.Parse(getString i)
        member this.GetByte i = Byte.Parse(getString i)
        member this.GetChar i = Char.Parse(getString i)
        member this.GetGuid i = Guid.Parse(getString i)
        member this.GetInt16 i = Int16.Parse(getString i)
        member this.GetInt32 i = Int32.Parse(getString i)
        member this.GetInt64 i = Int64.Parse(getString i)
        member this.GetFloat i = Single.Parse(getString i)
        member this.GetDouble i = Double.Parse(getString i)
        member this.GetString i = getString i
        member this.GetDecimal i = Decimal.Parse(getString i)
        member this.GetDateTime i = DateTime.Parse(getString i)
        member this.GetData i = raise(NotImplementedException())
        member this.IsDBNull i = (get i) = null
        member this.Read() = this.Read()
        member this.GetSchemaTable() = this.NotSupported()
        member this.GetBytes(i, fieldOffset, buffer, bufferOffset, length) = this.NotSupported()
        member this.GetChars(i, fieldOffset, buffer, bufferOffset, length) = this.NotSupported()
