using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TuzAzureStore.Models
{
    internal class AmapStreamReader
    {
        private readonly int _readSizeLimit;
        private readonly Stream _streamToRead;

        public int BytesRead { get; private set; }
        public byte[] Data { get; private set; }
        public bool LoadAborted { get; private set; }
        public Exception LoadAbortedException { get; private set; }

        public AmapStreamReader(int readSizeLimit, Stream streamToRead)
        {
            _readSizeLimit = readSizeLimit;
            _streamToRead = streamToRead;
        }

        public async Task Read(CancellationToken cancellationToken)
        {
            var remainingBytesToRead = _readSizeLimit;
            var byteBuffer = new byte[_readSizeLimit];
            var completeBuffer = new List<byte>();
            try
            {
                int bytesRead;
                do
                {
                    bytesRead = await _streamToRead.ReadAsync(byteBuffer, 0, remainingBytesToRead, cancellationToken);
                    completeBuffer.AddRange(byteBuffer.Take(bytesRead));
                    remainingBytesToRead -= bytesRead;

                } while (remainingBytesToRead > 0 && bytesRead > 0);
            }
            catch (Exception ex) when (ex is IOException ||
                                       ex is ObjectDisposedException ||
                                       ex is TaskCanceledException)
            {
                LoadAborted = true;
            }
            catch (Exception ex)
            {
                LoadAborted = true;
                LoadAbortedException = ex;
            }

            if (LoadAborted)
            {
                completeBuffer.AddRange(byteBuffer);

                var lastIndexOfNonNull = int.MaxValue;

                for (var i = completeBuffer.Count - 1; i > -1; i--)
                {
                    if (completeBuffer[i] != 0)
                    {
                        lastIndexOfNonNull = i;
                        break;
                    }
                }

                byteBuffer = completeBuffer.Take(lastIndexOfNonNull + 1).ToArray();
            }

            BytesRead = byteBuffer.Length;
            Data = byteBuffer;
        }
    }
}
