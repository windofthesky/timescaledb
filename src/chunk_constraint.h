#ifndef TIMESCALEDB_CHUNK_CONSTRAINT_H
#define TIMESCALEDB_CHUNK_CONSTRAINT_H


#include <postgres.h>
#include <nodes/pg_list.h>

#include "catalog.h"

typedef struct ChunkConstraint
{
	FormData_chunk_constraint fd;
} ChunkConstraint;


typedef struct ChunkConstraintVec
{
	int16		num_constraints;
	ChunkConstraint constraints[0];
}	ChunkConstraintVec;

typedef struct Chunk Chunk;
typedef struct DimensionSlice DimensionSlice;
typedef struct ChunkScanCtx ChunkScanCtx;

extern ChunkConstraint *chunk_constraint_scan_by_chunk_id(int32 chunk_id, Size num_constraints);
extern int	chunk_constraint_scan_by_dimension_slice_id(DimensionSlice *slice, ChunkScanCtx *ctx);
extern void chunk_constraint_insert_multi(ChunkConstraint *constraints, Size num_constraints);
extern ChunkConstraint *chunk_constraint_copy(ChunkConstraint *constraints, Size num_constraints);

#endif   /* TIMESCALEDB_CHUNK_CONSTRAINT_H */
