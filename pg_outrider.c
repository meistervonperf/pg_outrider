/* -------------------------------------------------------------------------
 *
 * pg_outrider.c
 *
 * Copyright (c) 2013-2016, PostgreSQL Global Development Group
 * Copyright (c) 2017,      Takashi Horikawa
 *
 * IDENTIFICATION
 *		pg_outrider/pg_outrider.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/heapam.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h" 
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "tcop/utility.h"
#include "commands/dbcommands.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_outrider_launch);

void		_PG_init(void);
void		pg_outrider_main(Datum) pg_attribute_noreturn();

#define MB2page(s) (s * ((1024 * 1024) / BLCKSZ))
#define naptimeQuantum 10
#define naptimeMaximum 5000

/* parameters to the worker */
typedef struct {
  Oid	database_id;
  Oid	authenticated_user_id;
  Oid	relOid;
  ForkNumber	forkNumber;
  int	incrementMB;
  int	watermarkMB;
} extendParam;

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static int	naptime = 10;
static int	interval = 0;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_outrider_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_outrider_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static bool 
pg_outrider_check_and_extend(extendParam *param)
{
	Page		page;
	Buffer		buffer;
	BlockNumber	blockNum = InvalidBlockNumber,
			firstBlock = InvalidBlockNumber;
	int64		wmblocks;
	Relation	relation;
	Size		freespace;
	int		i;
	static bool	rv = true;

	/* Open relation. */
	relation = relation_open(param->relOid, AccessShareLock);

	/* Acquire extenstion lock. */
	LockRelationForExtension(relation, ExclusiveLock);

	/* Inquire the block numbers in the relation and set the watermark_block. */
	wmblocks = RelationGetNumberOfBlocksInFork(relation, param->forkNumber) - MB2page(param->watermarkMB);
	if (wmblocks < 0) wmblocks = 0;

	if (GetRecordedFreeSpace(relation, wmblocks) != MaxHeapTupleSize)
	{
		for (i = 0; i < MB2page(param->incrementMB); ++i)
		{
			CHECK_FOR_INTERRUPTS();
	
			buffer = ReadBuffer(relation, P_NEW);
				
			/* Extend by one page. */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buffer);
			PageInit(page, BufferGetPageSize(buffer), 0);
			MarkBufferDirty(buffer);
			blockNum = BufferGetBlockNumber(buffer);
			freespace = PageGetHeapFreeSpace(page);
			UnlockReleaseBuffer(buffer);
	   
			/* Remember first block number thus added. */
			if (firstBlock == InvalidBlockNumber)
				firstBlock = blockNum;
	   
			/*
			 * Immediately update the bottom level of the FSM.  This has a good
			 * chance of making this page visible to other concurrently inserting
			 * backends, and we want that to happen without delay.
			 */
			RecordPageWithFreeSpace(relation, blockNum, freespace);
		}
	
		/*
		 * Updating the upper levels of the free space map is too expensive to do
		 * for every block, but it's worth doing once at the end to make sure that
		 * subsequent insertion activity sees all of those nifty free pages we
		 * just inserted.
		 *
		 * Note that we're using the freespace value that was reported for the
		 * last block we added as if it were the freespace value for every block
		 * we added.  That's actually true, because they're all equally empty.
		 */
		if (firstBlock != InvalidBlockNumber)
		{
			UpdateFreeSpaceMap(relation, firstBlock, blockNum, freespace);
			wmblocks = blockNum - MB2page(param->watermarkMB);
			if (wmblocks < 0) wmblocks = 0;
		}
	
		/* Rrelease extenstion lock. */
		UnlockRelationForExtension(relation, ExclusiveLock);
	
//		elog(LOG, "pg_outrider: relation size is extended from %d to %d, interval = %d.", firstBlock, blockNum, interval);
		interval = 0;

		rv = GetRecordedFreeSpace(relation, wmblocks) == MaxHeapTupleSize;

		if (!rv) naptime = ((naptime + naptimeQuantum) / (2 * naptimeQuantum)) * naptimeQuantum;
	}
	else
	{
		/* Rrelease extenstion lock. */
		UnlockRelationForExtension(relation, ExclusiveLock);
		if (rv && naptime < naptimeMaximum) naptime += naptimeQuantum;
		rv = true;
	}

	/* Close relation, release lock. */
	relation_close(relation, AccessShareLock);

	return rv;
}

void
pg_outrider_main(Datum main_arg)
{
  
	extendParam	*param = (extendParam *) &MyBgworkerEntry->bgw_extra;
	bool		canSleep = false;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_outrider_sighup);
	pqsignal(SIGTERM, pg_outrider_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnectionByOid(param->database_id, param->authenticated_user_id);

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		if (canSleep)
		{
			/*
			 * Background workers mustn't call usleep() or any direct equivalent:
			 * instead, they may wait on their process latch, which sleeps as
			 * necessary, but is awakened if postmaster dies.  That way the
			 * background process goes away immediately in an emergency.
			 */
			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						   naptime);
			ResetLatch(MyLatch);

			/* emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);

			/*
			 * In case of a SIGHUP, just reload the configuration.
			 */
			if (got_sighup)
			{
				got_sighup = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			interval += naptime;
		}

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();

		canSleep = pg_outrider_check_and_extend(param);

		CommitTransactionCommand();
	}
	proc_exit(1);
}

/*
 * Dynamically launch an pg_outrider worker.
 */
Datum
pg_outrider_launch(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	Relation	relation;
	AclResult	aclresult;
	extendParam	*param = (extendParam *) &worker.bgw_extra;
	pid_t		pid;

	/* Begin processing parameters.
	   Basic sanity checking. */
	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation cannot be null")));
	param->relOid = PG_GETARG_OID(0);
	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("incrementMB cannot be null"))));
	param->incrementMB = PG_GETARG_INT64(1);
	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 (errmsg("watermarkMB cannot be null"))));
	param->watermarkMB = PG_GETARG_INT64(2);
	/* End processing parameters. */

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;		/* new worker might not have library loaded */
	sprintf(worker.bgw_library_name, "pg_outrider");
	sprintf(worker.bgw_function_name, "pg_outrider_main");

	param->forkNumber = forkname_to_number("main");

	/* Open relation and check privileges. */
	relation = relation_open(param->relOid, AccessShareLock);
	aclresult = pg_class_aclcheck(param->relOid, GetUserId(), ACL_SELECT);
	if (aclresult != ACLCHECK_OK)
	{
		aclcheck_error(aclresult, ACL_KIND_CLASS, get_rel_name(param->relOid));
		relation_close(relation, AccessShareLock);		
		PG_RETURN_INT32(0);		
	}

	/* Check that the fork exists. */
	RelationOpenSmgr(relation);
	if (!smgrexists(relation->rd_smgr, param->forkNumber))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("fork \"main\" does not exist for this relation")));
		relation_close(relation, AccessShareLock);		
		PG_RETURN_INT32(0);		
	}

	snprintf(worker.bgw_name, BGW_MAXLEN, "outrider (%s)", RelationGetRelationName(relation));
	relation_close(relation, AccessShareLock);

	param->database_id = MyDatabaseId;
	param->authenticated_user_id = GetAuthenticatedUserId();
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
			   errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			  errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	PG_RETURN_INT32(pid);
}
