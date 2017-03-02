using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using NUnit.Framework;
using EventStore.Core.Messaging;
using EventStore.Core.Messages;
using EventStore.Common.Utils;
using System.Collections.Generic;

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
            Assert.IsTrue((bool)completeMsg0.wasScavenged, "Chunk 0 was not scavenged");
            Assert.IsEmpty(completeMsg0.errorMessage, "Chunk 0 errored");

            var completeMsg1 = chunk1CompleteMsg.Events[0].Data.ParseJson<dynamic>();
            Assert.IsTrue((bool)completeMsg1.wasScavenged, "Chunk 1 was not scavenged");
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
}