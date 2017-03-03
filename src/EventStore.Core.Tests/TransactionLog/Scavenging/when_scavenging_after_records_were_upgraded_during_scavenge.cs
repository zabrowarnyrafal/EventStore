using System;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using NUnit.Framework;
using EventStore.Core.Messaging;
using EventStore.Core.Messages;
using EventStore.Common.Utils;
using System.Collections.Generic;
using EventStore.Core.Tests.Services.Storage;

namespace EventStore.Core.Tests.TransactionLog.Scavenging
{
    [TestFixture]
    public class when_scavenging_after_records_were_upgraded_during_scavenge: ScavengeTestScenario
    {
        private const byte _recordVersion = LogRecordVersion.LogRecordV0;
        private List<Message> _scavengeMessages;

        protected override DbResult CreateDb(TFChunkDbCreationHelper dbCreator)
        {
            return dbCreator
                    .Chunk(Rec.Prepare(0, "bla", version: _recordVersion),
                           Rec.Prepare(1, "bla", version: _recordVersion),
                           Rec.Commit(0, "bla", version: _recordVersion))
                    .Chunk(Rec.Prepare(2, "bla3", version: _recordVersion),
                           Rec.Prepare(2, "bla3", version: _recordVersion),
                           Rec.Commit(1, "bla", version: _recordVersion),
                           Rec.Commit(2, "bla3", version: _recordVersion))
                    .CompleteLastChunk()
                    .CreateDb();
        }

        public override void AfterScavenge()
        {
            _scavengeMessages = Scavenge();
        }

        protected override LogRecord[][] KeptRecords(DbResult dbResult)
        {
            return dbResult.Recs;
        }

        [Test]
        public void should_receive_scavenge_chunk_complete_events_with_no_errors()
        {
            Assert.AreEqual(2, _scavengeMessages.Count);
            var chunk0CompleteMsg = _scavengeMessages[0] as ClientMessage.WriteEvents;
            var chunk1CompleteMsg = _scavengeMessages[1] as ClientMessage.WriteEvents;

            var completeMsg0 = chunk0CompleteMsg.Events[0].Data.ParseJson<dynamic>();
            Assert.AreEqual("", (string)completeMsg0.errorMessage, "Chunk 0 errored");
            Assert.IsEmpty(completeMsg0.errorMessage, "Chunk 0 errored");

            var completeMsg1 = chunk1CompleteMsg.Events[0].Data.ParseJson<dynamic>();
            Assert.AreEqual("", (string)completeMsg1.errorMessage, "Chunk 1 errored");
            Assert.IsEmpty(completeMsg1.errorMessage, "Chunk 1 errored");
        }

        [Test]
        public void all_records_are_upgraded()
        {
            CheckRecordsV0(true);
        }

        [Test]
        public void chunk_footer_is_upgrade_should_be_true()
        {
            var chunk0 = Db.Manager.GetChunk(0);
            var chunk1 = Db.Manager.GetChunk(1);

            Assert.IsTrue(chunk0.ChunkFooter.IsUpgrade, "Chunk 0 IsUpgrade");
            Assert.IsTrue(chunk1.ChunkFooter.IsUpgrade, "Chunk 1 IsUpgrade");
        }
    }

    [TestFixture]
    [Category("UnderTest")]
    public class when_upgrading_records_during_scavenge_and_records_will_be_written_at_positions_greater_than_max_chunk_size
        : ReadIndexTestScenario
    {
        protected override void WriteTestScenario()
        {
            long position = 0;
            int count = 0;
            while (position < 20000) // 2 chunks
            {
                var record = WriteSingleEventWithLogVersion0(Guid.NewGuid(), "ES1", WriterCheckpoint.ReadNonFlushed(), count);
                position = record.LogPosition;
                count++;
            }

            Scavenge(false, false);
        }

        [Test]
        public void the_scavenge_should_complete_without_errors()
        {
            Assert.AreEqual(2, ScavengeMessages.Count);
            var chunk0CompleteMsg = ScavengeMessages[0] as ClientMessage.WriteEvents;
            var chunk1CompleteMsg = ScavengeMessages[1] as ClientMessage.WriteEvents;

            var completeMsg0 = chunk0CompleteMsg.Events[0].Data.ParseJson<dynamic>();
            Assert.AreEqual("", (string)completeMsg0.errorMessage, "Chunk 0 errored");
            Assert.IsTrue((bool)completeMsg0.wasScavenged, "Chunk 0 was not scavenged");

            var completeMsg1 = chunk1CompleteMsg.Events[0].Data.ParseJson<dynamic>();
            Assert.AreEqual("", (string)completeMsg1.errorMessage, "Chunk 1 errored");
            Assert.IsTrue((bool)completeMsg1.wasScavenged, "Chunk 1 was not scavenged");
        }
    }
}