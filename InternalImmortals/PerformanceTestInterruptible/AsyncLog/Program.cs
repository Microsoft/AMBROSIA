using Ambrosia;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncLog
{
    public struct LSN
    {
        public long sequenceID;
        public long epochID;
        public int logID;

        public bool LessThanOrEqualTo(LSN other)
        {
            Debug.Assert(this.logID == other.logID);

            return (this.epochID < other.epochID) ||
                (this.epochID == other.epochID && this.sequenceID <= other.sequenceID);
        }
    }

    public struct VectorTimestamp
    {
        public ValueTuple<long, long>[] payload;
        public int length;
    }

    public enum SleepStatus
    {
        ASLEEP = 0,
        AWAKE = 1,
    }

    public interface ILog
    {
        Task<LSN> AppendAsync(byte[] payload, int size, LSN[] dependencies, int numDependencies);
        Task<LSN> SleepAsync();
        Task WakeupAsync();
        SleepStatus SleepStatus();
        void SwitchLogStreams(ILogWriter newLogStream);
        long LogfileSize();
    }

    public interface ILogAppendClient
    {
        Task<ValueTuple<byte[], int>> BeforeEpochAsync(LSN lsn);
        Task AfterEpochAsync(LSN lsn);
        Task Commit(LSN lsn);
    }


    internal static class Constants
    {
        // Size of cache line in bytes
        public const int kCacheLineBytes = 64;
        public const long kHeaderLockMask = 0x000000001;
        public const int kHeaderLockShift = 1;
    }

    // Struct to manage payload and vector timestamp buffers as one atomic unit.
    [StructLayout(LayoutKind.Explicit, Size = Constants.kCacheLineBytes)]
    internal struct LogLengths
    {
        [FieldOffset(0)]
        public long header;
        [FieldOffset(8)]
        public long payloadLength;
        [FieldOffset(16)]
        public long timestampLength;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTuple<long, long, long> AtomicRead()
        {
            long beforeHeader = 0x1;
            long afterHeader = 0x1;
            long curPayload = -1;
            long curTimestamp = -1;
            do
            {
                beforeHeader = Interlocked.Read(ref header);
                curPayload = payloadLength;
                curTimestamp = timestampLength;
                afterHeader = Interlocked.Read(ref header);
            } while ((beforeHeader == afterHeader) && ((beforeHeader & Constants.kHeaderLockMask) == 0));

            return ValueTuple.Create<long, long, long>((beforeHeader >> Constants.kHeaderLockShift), curPayload, curTimestamp);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AtomicWrite(long newHeader, long newPayloadLen, long newTimestampLen)
        {
            long lockedNewHeader = ((newHeader << Constants.kHeaderLockShift) | Constants.kHeaderLockMask);
            long unlockedNewHeader = (newHeader << Constants.kHeaderLockShift);

            long oldHeader = Interlocked.Exchange(ref header, lockedNewHeader);
            Debug.Assert(((oldHeader >> Constants.kHeaderLockShift) + 1) == newHeader && ((oldHeader & Constants.kHeaderLockMask) == 0));
            payloadLength = newPayloadLen;
            timestampLength = newTimestampLen;
            oldHeader = Interlocked.Exchange(ref header, unlockedNewHeader);
            Debug.Assert(oldHeader == lockedNewHeader);
        }
    }

    public class AmbrosiaLog : ILog
    {
        private ILogAppendClient _appendClient;
        private Task _clientDurabilityTask;

        private ILogWriter _logStream;
        private int _logID;
        private long _lastEpochSequenceID;
        private long _epochID;

        private long _completedWrites;

        private Task _durabilityTask;

        private bool _persistLogs;

        byte[] _buf;
        volatile byte[] _bufbak;
        long _maxBufSize;
        internal const int HeaderSize = 40;

        //  Header format:
        //  ----------------------
        //  | CommitterID        |
        //  | 4 bytes            |
        //  ----------------------
        //  | PageSize           |
        //  | 4 bytes            |
        //  ----------------------
        //  | CheckBytes         |
        //  | 8 bytes            |
        //  ----------------------
        //  | EpochID            |
        //  | 8 bytes            |
        //  ----------------------
        //  | MinSequenceID      |
        //  | 8 bytes            |
        //  ----------------------
        //  | MaxSequenceID      |
        //  | 8 bytes            |
        //  ----------------------

        byte[] _checkTempBytes = new byte[8];
        byte[] _checkTempBytes2 = new byte[8];

        Stream _localListener;

        // Used in CAS. The first 31 bits are the #of writers, the next 32 bits is the buffer size, the last bit is the sealed bit
        long _status;
        const int SealedBits = 1;
        const int TailBits = 32;
        const int numWritesBits = 31;
        const long Last32Mask = 0x00000000FFFFFFFF;
        const long First32Mask = Last32Mask << 32;

        ConcurrentDictionary<long, LSN> _pageDeps;
        ConcurrentDictionary<long, LSN> _pageDepsBak;

        public AmbrosiaLog(int logID, Stream localListener, ILogAppendClient client, bool persistLogs, long maxBufSize = 8 * 1024 * 1024)
        {
            _logID = logID;
            _status = HeaderSize << SealedBits;
            _appendClient = client;
            _persistLogs = persistLogs;
            _maxBufSize = maxBufSize;

            _buf = new byte[maxBufSize];
            _bufbak = new byte[maxBufSize];

            _pageDeps = new ConcurrentDictionary<long, LSN>();
            _pageDepsBak = new ConcurrentDictionary<long, LSN>();

            _epochID = 0;
            _lastEpochSequenceID = -1;

            var memStream = new MemoryStream(_buf);
            var memStreamBak = new MemoryStream(_bufbak);
            memStream.WriteIntFixed(logID);
            memStreamBak.WriteIntFixed(logID);

            _logStream = null;
            _localListener = localListener;
        }

        internal unsafe long CheckBytes(byte[] bufToCalc,
                                        int offset,
                                        int length)
        {
            long checkBytes = 0;

            fixed (byte* p = bufToCalc)
            {
                if (offset % 8 == 0)
                {
                    int startLongCalc = offset / 8;
                    int numLongCalcs = length / 8;
                    int numByteCalcs = length % 8;
                    long* longPtr = ((long*)p) + startLongCalc;
                    for (int i = 0; i < numLongCalcs; i++)
                    {
                        checkBytes ^= longPtr[i];
                    }
                    if (numByteCalcs != 0)
                    {
                        var lastBytes = (byte*)(longPtr + numLongCalcs);
                        for (int i = 0; i < 8; i++)
                        {
                            if (i < numByteCalcs)
                            {
                                _checkTempBytes[i] = lastBytes[i];
                            }
                            else
                            {
                                _checkTempBytes[i] = 0;
                            }
                        }
                        fixed (byte* p2 = _checkTempBytes)
                        {
                            checkBytes ^= *((long*)p2);
                        }
                    }
                }
                else
                {
                    // Jose: This is a hack. Fix it for real.
                    Trace.Assert(false);
                    // _myAmbrosia.OnError(0, "checkbytes case not implemented 2");
                }
            }
            return checkBytes;
        }

        internal unsafe long CheckBytes(int offset,
                                int length)
        {
            long checkBytes = 0;

            fixed (byte* p = _buf)
            {
                if (offset % 8 == 0)
                {
                    int startLongCalc = offset / 8;
                    int numLongCalcs = length / 8;
                    int numByteCalcs = length % 8;
                    long* longPtr = ((long*)p) + startLongCalc;
                    for (int i = 0; i < numLongCalcs; i++)
                    {
                        checkBytes ^= longPtr[i];
                    }
                    if (numByteCalcs != 0)
                    {
                        var lastBytes = (byte*)(longPtr + numLongCalcs);
                        for (int i = 0; i < 8; i++)
                        {
                            if (i < numByteCalcs)
                            {
                                _checkTempBytes[i] = lastBytes[i];
                            }
                            else
                            {
                                _checkTempBytes[i] = 0;
                            }
                        }
                        fixed (byte* p2 = _checkTempBytes)
                        {
                            checkBytes ^= *((long*)p2);
                        }
                    }
                }
                else
                {
                    // Jose: This is a hack. Fix it for real.
                    Trace.Assert(false);
                    // _myAmbrosia.OnError(0, "checkbytes case not implemented 2");
                }
            }
            return checkBytes;
        }

        internal unsafe long CheckBytesExtra(int offset,
                                             int length,
                                             byte[] extraBytes,
                                             int extraLength)
        {
            var firstBufferCheck = CheckBytes(offset, length);
            var secondBufferCheck = CheckBytes(extraBytes, 0, extraLength);
            long shiftedSecondBuffer = secondBufferCheck;
            var lastByteLongOffset = length % 8;
            if (lastByteLongOffset != 0)
            {
                fixed (byte* p = _checkTempBytes)
                {
                    *((long*)p) = secondBufferCheck;
                }
                // Create new buffer with circularly shifted secondBufferCheck
                for (int i = 0; i < 8; i++)
                {
                    _checkTempBytes2[i] = _checkTempBytes[(i - lastByteLongOffset + 8) % 8];
                }
                fixed (byte* p = _checkTempBytes2)
                {
                    shiftedSecondBuffer = *((long*)p);
                }
            }
            return firstBufferCheck ^ shiftedSecondBuffer;
        }

        public async Task<LSN> SleepAsync()
        {
            while (true)
            {
                // We're going to try to seal the buffer to prevent any more appends.
                var localStatus = Interlocked.Read(ref _status);
                while (localStatus % 2 == 1)
                {
                    await Task.Yield();
                    localStatus = Interlocked.Read(ref _status);
                }

                var newLocalStatus = localStatus + 1;
                var origVal = Interlocked.CompareExchange(ref _status, newLocalStatus, localStatus);
                if (origVal == localStatus)
                {
                    // Sealed the active buffer, wait for prior buff to be made durable.
                    while (_bufbak == null)
                    {
                        await Task.Yield();
                    }

                    // Wait for all outstanding writes to complete.
                    var numWrites = (newLocalStatus >> (64 - numWritesBits));
                    var completedWrites = Interlocked.Read(ref _completedWrites);
                    while (numWrites < completedWrites)
                    {
                        await Task.Yield();
                        completedWrites = Interlocked.Read(ref _completedWrites);
                    }

                    LSN retLSN;
                    retLSN.sequenceID = _lastEpochSequenceID + numWrites;
                    retLSN.epochID = _epochID;
                    retLSN.logID = _logID;
                    return retLSN;
                }
            }
        }

        public async Task WakeupAsync()
        {
            var localStatus = Interlocked.Read(ref _status);
            if (localStatus % 2 == 0 || _bufbak == null)
            {
                // The log wasn't actually asleep!
                Trace.Assert(false);
            }
            // We're going to try to unseal the buffer
            var newLocalStatus = localStatus - 1;
            var origVal = Interlocked.CompareExchange(ref _status, newLocalStatus, localStatus);
            // Check if the compare and swap succeeded
            if (origVal != localStatus)
            {
                // The log wasn't actually asleep!
                Trace.Assert(false);
            }
        }

        private void AddDependencies(ConcurrentDictionary<long, LSN> depDict, LSN[] dependencies, int num_dependencies)
        {
            for (int i = 0; i < num_dependencies; ++i)
            {
                // We're only accounting for dependencies on other logs. Local dependencies are implicitly handled via log ordering.
                Debug.Assert(dependencies[i].logID != _logID);

                if (!depDict.TryAdd(dependencies[i].logID, dependencies[i]))
                {
                    LSN currentDep = depDict[dependencies[i].logID];
                    while (!dependencies[i].LessThanOrEqualTo(currentDep))
                    {
                        if (depDict.TryUpdate(dependencies[i].logID, dependencies[i], currentDep))
                        {
                            break;
                        }
                        currentDep = depDict[dependencies[i].logID];
                    }
                }
            }
        }

        public async Task<VectorTimestamp> GetVectorTimestamp(long localIndex)
        {
            Debug.Assert(false);
        }

        public async Task<LSN> AppendAsync(byte[] payload, int length, VectorTimestamp dependencies)
        {
            while (true)
            {
                bool sealing = false;
                long localStatus;
                localStatus = Interlocked.Read(ref _status);

                // Yield if the sealed bit is set
                while (localStatus % 2 == 1)
                {
                    await Task.Yield();
                    localStatus = Interlocked.Read(ref _status);
                }
                var oldBufLength = ((localStatus >> SealedBits) & Last32Mask);
                var newLength = oldBufLength + length;

                // Assemble the new status 
                long cmpxchngLocalStatus;
                if ((newLength > _maxBufSize) || (_bufbak != null))
                {
                    // We're going to try to seal the buffer
                    cmpxchngLocalStatus = localStatus + 1;
                    sealing = true;
                }
                else
                {
                    // We're going to try to add to the end of the existing buffer
                    var newWriteCount = (localStatus >> (64 - numWritesBits)) + 1;
                    cmpxchngLocalStatus = ((newWriteCount) << (64 - numWritesBits)) | (newLength << SealedBits);
                }
                var origVal = Interlocked.CompareExchange(ref _status, cmpxchngLocalStatus, localStatus);

                // Check if the compare and swap succeeded, otherwise try again
                if (origVal == localStatus)
                {
                    LSN retLSN;

                    retLSN.logID = _logID;
                    retLSN.epochID = _epochID;

                    // This call successfully sealed the buffer. Remember we still have an extra
                    // message to take care of
                    if (sealing)
                    {
                        // We have just filled the backup buffer and must wait until any other commit finishes
                        
                        int counter = 0;
                        while (_bufbak == null)
                        {
                            counter++;
                            if (counter == 100000)
                            {
                                counter = 0;
                                await Task.Yield();
                            }
                        }

                        // There is no other write going on. Take the backup buffer
                        var newWriteBuf = _bufbak;
                        var newPageDeps = _pageDepsBak;
                        _bufbak = null;
                        _pageDepsBak = null;

                        // Wait for in-flight writes to complete
                        var expectedWrites = (cmpxchngLocalStatus >> (64 - numWritesBits));
                        var completedWrites = Interlocked.Read(ref _completedWrites);
                        while (expectedWrites != completedWrites)
                        {
                            await Task.Yield();
                            completedWrites = Interlocked.Read(ref _completedWrites);
                        }

                        // Filling header with enough info to detect incomplete writes and also writing the page length
                        var writeStream = new MemoryStream(_buf, 4, 36);
                        int lengthOnPage;
                        if (newLength <= _maxBufSize)
                        {
                            lengthOnPage = (int)newLength;
                        }
                        else
                        {
                            lengthOnPage = (int)oldBufLength;
                        }
                        writeStream.WriteIntFixed(lengthOnPage);

                        if (newLength <= _maxBufSize)
                        {
                            // Copy the contents into the log record buffer
                            Buffer.BlockCopy(payload, 0, _buf, (int)oldBufLength, length);
                        }
                        long checkBytes;
                        if (length <= (_maxBufSize - HeaderSize))
                        {
                            // new message will end up in a commit buffer. Use normal CheckBytes
                            checkBytes = CheckBytes(HeaderSize, lengthOnPage - HeaderSize);
                        }
                        else
                        {
                            // new message is too big to land in a commit buffer and will be tacked on the end.
                            checkBytes = CheckBytesExtra(HeaderSize, lengthOnPage - HeaderSize, payload, length);
                        }
                        writeStream.WriteLongFixed(checkBytes);
                        writeStream.WriteLongFixed(_epochID);
                        writeStream.WriteLongFixed(_lastEpochSequenceID + 1);
                        writeStream.WriteLongFixed(_lastEpochSequenceID + completedWrites + 1);

                        // Do the actual commit
                        long newLocalStatus;
                        if (newLength <= _maxBufSize)
                        {
                            // add event to current buffer and commit
                            AddDependencies(_pageDeps, dependencies, num_dependencies);
                            _durabilityTask = HardenAsync(_buf, (int)newLength, _pageDeps);
                            newLocalStatus = HeaderSize << SealedBits;
                        }
                        else if (length > (_maxBufSize - HeaderSize))
                        {
                            // commit the current buffer and tack on the current event at the end
                            AddDependencies(_pageDeps, dependencies, num_dependencies);
                            _durabilityTask = HardenAsync(_buf, (int)oldBufLength, payload, length, _pageDeps);
                            newLocalStatus = HeaderSize << SealedBits;
                        }
                        else
                        {
                            // commit and add new event to new buffer
                            AddDependencies(newPageDeps, dependencies, num_dependencies);
                            _durabilityTask = HardenAsync(_buf, (int)oldBufLength, _pageDeps);
                            Buffer.BlockCopy(payload, 0, newWriteBuf, (int)HeaderSize, length);
                            newLocalStatus = (HeaderSize + length) << SealedBits;
                        }

                        retLSN.sequenceID = _lastEpochSequenceID + completedWrites + 1;

                        // Setup state for the new epoch
                        _epochID += 1;
                        _lastEpochSequenceID += completedWrites + 1;
                        _buf = newWriteBuf;
                        _pageDeps = newPageDeps;
                        _completedWrites = 0;

                        // Unlock the log for new appends
                        var oldStatus = Interlocked.Exchange(ref _status, newLocalStatus);
                        Debug.Assert(oldStatus == cmpxchngLocalStatus);

                        return retLSN;
                    }

                    AddDependencies(_pageDeps, dependencies, num_dependencies);

                    // Add the message to the existing buffer
                    Buffer.BlockCopy(payload, 0, _buf, (int)oldBufLength, length);
                    // Update completed write count.
                    Interlocked.Increment(ref _completedWrites);
                    localStatus = Interlocked.Read(ref _status);
                    if (localStatus % 2 == 0 && _bufbak != null)
                    {
                        await TryHardenAsync();
                    }

                    retLSN.sequenceID = _lastEpochSequenceID + (cmpxchngLocalStatus >> (64 - numWritesBits));
                    return retLSN;
                }
            }
        }

        private async Task HardenAsync(byte[] buf, int length, ConcurrentDictionary<long, LSN> pageDeps)
        {
            Debug.Assert(_bufbak == null && _pageDepsBak == null);

            LSN lastLSN;
            lastLSN.sequenceID = _completedWrites + _lastEpochSequenceID + 1;
            lastLSN.epochID = _epochID;
            lastLSN.logID = _logID;

            try
            {
                // Get the client's footer to associate with the epoch.
                var clientFooter = await _appendClient.BeforeEpochAsync(lastLSN);

                // Send committed page contents to local listener.
                if (_localListener != null)
                {
                    await _localListener.WriteAsync(buf, 0, length);
                    var flushtask = _localListener.FlushAsync();
                }

                // writes to _logstream - don't want to persist logs when perf testing so this is optional parameter
                if (_persistLogs)
                {
                    await _logStream.WriteAsync(buf, 0, length);

                    // Write out the client's footer
                    _logStream.WriteIntFixed(clientFooter.Item2);
                    await _logStream.WriteAsync(clientFooter.Item1, 0, clientFooter.Item2);

                    // Write out dependency meta-data
                    WriteDependencies(pageDeps);

                    await _logStream.FlushAsync();
                }
            }
            catch (Exception e)
            {
                throw e;
            }

            // Signal epoch durability to the client.
            _clientDurabilityTask = _appendClient.AfterEpochAsync(lastLSN);

            _bufbak = buf;
            pageDeps.Clear();
            _pageDepsBak = pageDeps;
            await TryHardenAsync();
        }

        private void WriteDependencies(ConcurrentDictionary<long, LSN> pageDeps)
        {
            _logStream.WriteIntFixed(pageDeps.Count);
            foreach (var kv in pageDeps)
            {
                _logStream.WriteLongFixed(kv.Value.logID);
                _logStream.WriteLongFixed(kv.Value.epochID);
                _logStream.WriteLongFixed(kv.Value.sequenceID);
            }
        }

        // This method switches the log stream to the provided stream and removes the write lock on the old file
        public void SwitchLogStreams(ILogWriter newLogStream)
        {
            if (_status % 2 != 1 || _bufbak == null)
            {
                Trace.Assert(false);
            }

            if (_logStream != null)
            {
                _logStream.Dispose();
            }
            _logStream = newLogStream;
        }

        public SleepStatus SleepStatus()
        {
            if (_status % 2 != 1 || _bufbak == null)
            {
                return AsyncLog.SleepStatus.AWAKE;
            }
            else
            {
                return AsyncLog.SleepStatus.ASLEEP;
            }
        }

        private async Task HardenAsync(byte[] firstBufToCommit,
                          int length1,
                          byte[] secondBufToCommit,
                          int length2,
                          ConcurrentDictionary<long, LSN> pageDeps)
        {
            Debug.Assert(_bufbak == null && _pageDepsBak == null);

            LSN lastLSN;
            lastLSN.sequenceID = _completedWrites + _lastEpochSequenceID + 1;
            lastLSN.epochID = _epochID;
            lastLSN.logID = _logID;

            try
            {
                // Get the client's footer to associate with the epoch
                var clientFooter = await _appendClient.BeforeEpochAsync(lastLSN);

                // Send committed page contents to local listener
                if (_localListener != null)
                {
                    // We're going to write a hand-crafted header to fix up the length
                    _localListener.Write(firstBufToCommit, 0, 4);
                    _localListener.WriteIntFixed(length1 + length2);
                    _localListener.Write(firstBufToCommit, 8, 32);
                    await _localListener.WriteAsync(firstBufToCommit, HeaderSize, length1 - HeaderSize);
                    await _localListener.WriteAsync(secondBufToCommit, 0, length2);
                    var flushtask = _localListener.FlushAsync();
                }

                // writes to _logstream - don't want to persist logs when perf testing so this is optional parameter
                if (_persistLogs)
                {
                    // Write out the payload
                    _logStream.Write(firstBufToCommit, 0, 4);
                    _logStream.WriteIntFixed(length1 + length2);
                    _logStream.Write(firstBufToCommit, 8, 32);
                    await _logStream.WriteAsync(firstBufToCommit, HeaderSize, length1 - HeaderSize);
                    await _logStream.WriteAsync(secondBufToCommit, 0, length2);

                    // Write out the client's footer
                    _logStream.WriteIntFixed(clientFooter.Item2);
                    await _logStream.WriteAsync(clientFooter.Item1, 0, clientFooter.Item2);

                    // Write out dependency information
                    WriteDependencies(pageDeps);

                    await _logStream.FlushAsync();
                }
            }
            catch (Exception e)
            {
                throw e;
            }

            // Signal epoch durability to the client.
            _clientDurabilityTask = _appendClient.AfterEpochAsync(lastLSN);

            _bufbak = firstBufToCommit;
            pageDeps.Clear();
            _pageDepsBak = pageDeps;
            await TryHardenAsync();
        }

        public async Task TryHardenAsync()
        {
            long localStatus;
            localStatus = Interlocked.Read(ref _status);

            var bufLength = ((localStatus >> SealedBits) & Last32Mask);
            // give up and try later if the sealed bit is set or there is nothing to write
            if (localStatus % 2 == 1 || bufLength == HeaderSize || _bufbak == null)
            {
                return;
            }

            // Assemble the new status 
            long newLocalStatus;
            newLocalStatus = localStatus + 1;
            var origVal = Interlocked.CompareExchange(ref _status, newLocalStatus, localStatus);

            // Check if the compare and swap succeeded, otherwise skip flush
            if (origVal == localStatus)
            {
                // This call successfully sealed the buffer.

                // We have just filled the backup buffer and must wait until any other commit finishes
                int counter = 0;
                while (_bufbak == null)
                {
                    counter++;
                    if (counter == 100000)
                    {
                        counter = 0;
                        await Task.Yield();
                    }
                }

                // There is no other write going on. Take the backup buffer
                var newWriteBuf = _bufbak;
                var newPageDeps = _pageDepsBak;

                _bufbak = null;
                _pageDepsBak = null;

                // Wait for in-flight writes to complete
                var expectedWrites = (newLocalStatus >> (64 - numWritesBits));
                var completedWrites = Interlocked.Read(ref _completedWrites);
                while (expectedWrites != completedWrites)
                {
                    await Task.Yield();
                    completedWrites = Interlocked.Read(ref _completedWrites);
                }

                // Filling header with enough info to detect incomplete writes and also writing the page length
                var writeStream = new MemoryStream(_buf, 4, 36);
                writeStream.WriteIntFixed((int)bufLength);
                long checkBytes = CheckBytes(HeaderSize, (int)bufLength - HeaderSize);
                writeStream.WriteLongFixed(checkBytes);
                writeStream.WriteLongFixed(_epochID);
                writeStream.WriteLongFixed(_lastEpochSequenceID + 1);
                writeStream.WriteLongFixed(_lastEpochSequenceID + completedWrites);

                // Make the page durable asynchronously
                _durabilityTask = HardenAsync(_buf, (int)bufLength, _pageDeps);

                // Set things up for the new epoch
                _lastEpochSequenceID += completedWrites;
                _epochID += 1;
                _buf = newWriteBuf;
                _pageDeps = newPageDeps;
                _completedWrites = 0;

                // Unlock the log for appends
                newLocalStatus = HeaderSize << SealedBits;
                Interlocked.Exchange(ref _status, newLocalStatus);
            }
        }

        public long LogfileSize()
        {
            return (long)_logStream.FileSize;
        }
    }
}
