-- The journal's two tables. Apply once to the database the journal is created on
-- (or derive the same from `table.ts` with drizzle-kit).
CREATE TABLE `journal` (
	`seq` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`eventId` text NOT NULL,
	`streamType` text NOT NULL,
	`streamId` text NOT NULL,
	`type` text NOT NULL,
	`v` integer NOT NULL,
	`ev` integer NOT NULL,
	`payload` text NOT NULL,
	`origin` text NOT NULL,
	`at` text NOT NULL,
	`recordedAt` text NOT NULL,
	`via` text,
	`causedBy` integer
);

CREATE UNIQUE INDEX `journal_eventId` ON `journal` (`eventId`);
CREATE INDEX `journal_stream_seq` ON `journal` (`streamType`,`streamId`,`seq`);
CREATE INDEX `journal_stream_type` ON `journal` (`streamType`,`streamId`,`type`);
CREATE TABLE `journal_meta` (
	`streamType` text NOT NULL,
	`streamId` text NOT NULL,
	`projectionVersions` text,
	`appliedThroughSeq` integer DEFAULT 0 NOT NULL,
	`rebuiltAt` text,
	PRIMARY KEY(`streamType`, `streamId`)
);
