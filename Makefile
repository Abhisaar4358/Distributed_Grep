
CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -Wpedantic -Iinclude
LDFLAGS  := -lpthread -lstdc++fs

SRCDIR   := src
OBJDIR   := build/obj
BINDIR   := build/bin

# Directories used by the demo run
DATA_DIR  := data
OUT_DIR   := output
INTER_DIR := intermediate

PATTERN   := XQZ
R_COUNT   := 3

# Source to Object mappings 
COMMON_SRC := $(SRCDIR)/mapreduce.cpp
COMMON_OBJ := $(OBJDIR)/mapreduce.o

MASTER_OBJ := $(OBJDIR)/master.o
WORKER_OBJ := $(OBJDIR)/worker.o
GENDAT_OBJ := $(OBJDIR)/generate_data.o

MASTER_BIN     := $(BINDIR)/master
WORKER_BIN     := $(BINDIR)/worker
GENERATE_BIN   := $(BINDIR)/generate_data

# Default target 
.PHONY: all
all: $(MASTER_BIN) $(WORKER_BIN) $(GENERATE_BIN)
	@echo ""
	@echo "╔══════════════════════════════════════════╗"
	@echo "║  Build successful!                       ║"
	@echo "║  Binaries in: $(BINDIR)/               ║"
	@echo "╚══════════════════════════════════════════╝"

# Directory creation 
$(OBJDIR) $(BINDIR):
	mkdir -p $@

# Compile rules 
$(COMMON_OBJ): $(COMMON_SRC) include/mapreduce.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(MASTER_OBJ): $(SRCDIR)/master.cpp include/mapreduce.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(WORKER_OBJ): $(SRCDIR)/worker.cpp include/mapreduce.h | $(OBJDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(GENDAT_OBJ): $(SRCDIR)/generate_data.cpp | $(OBJDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Link rules 
$(MASTER_BIN): $(MASTER_OBJ) $(COMMON_OBJ) | $(BINDIR)
	$(CXX) $^ -o $@ $(LDFLAGS)
	@echo "  Linked: $@"

$(WORKER_BIN): $(WORKER_OBJ) $(COMMON_OBJ) | $(BINDIR)
	$(CXX) $^ -o $@ $(LDFLAGS)
	@echo "  Linked: $@"

$(GENERATE_BIN): $(GENDAT_OBJ) | $(BINDIR)
	$(CXX) $^ -o $@ $(LDFLAGS)
	@echo "  Linked: $@"

#  Aliases
.PHONY: master worker generate_data
master:       $(MASTER_BIN)
worker:       $(WORKER_BIN)
generate_data:$(GENERATE_BIN)

#  Clean 
.PHONY: clean
clean:
	rm -rf build $(DATA_DIR) $(OUT_DIR) $(INTER_DIR)
	@echo "Cleaned."

# run_demo – automated end-to-end test
#
#  1. Generate 10 split files (5 000 lines each, pattern = XQZ)
#  2. Start the master in the background
#  3. Start 3 workers in the background
#  4. Wait for the master to exit (all work done)
#  5. Merge & verify output

.PHONY: run_demo
run_demo: all
	@echo ""
	@echo "━━━ Step 1: Generate data ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	mkdir -p $(DATA_DIR) $(OUT_DIR) $(INTER_DIR)
	$(GENERATE_BIN) $(DATA_DIR) 10 5000 $(PATTERN)

	@echo ""
	@echo "━━━ Step 2: Start Master ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	$(MASTER_BIN) $(PATTERN) $(DATA_DIR) $(OUT_DIR) $(INTER_DIR) $(R_COUNT) &

	@sleep 1

	@echo ""
	@echo "━━━ Step 3: Start 3 Workers ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	$(WORKER_BIN) 127.0.0.1 &
	$(WORKER_BIN) 127.0.0.1 &
	$(WORKER_BIN) 127.0.0.1 &

	@echo "Workers started. Waiting for job to complete…"
	@wait

	@echo ""
	@echo "━━━ Step 4: Verify output ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "Lines containing '$(PATTERN)' in output:"
	@cat $(OUT_DIR)/output-*.txt 2>/dev/null | grep -c "$(PATTERN)" || true
	@echo ""
	@echo "Sample matched lines:"
	@cat $(OUT_DIR)/output-*.txt 2>/dev/null | grep "$(PATTERN)" | head -10 || true
	@echo ""
	@echo "━━━ Demo complete ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Help 
.PHONY: help
help:
	@echo "Distributed Grep MapReduce – Makefile targets:"
	@echo "  all           Build all binaries (default)"
	@echo "  master        Build just the master"
	@echo "  worker        Build just the worker"
	@echo "  generate_data Build just the data generator"
	@echo "  run_demo      End-to-end demo (generate → master → 3 workers → verify)"
	@echo "  clean         Remove all build artefacts and generated data"
	@echo "  help          Show this message"
