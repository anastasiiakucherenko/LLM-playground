#!/bin/bash
#SBATCH --job-name=es-search-pipeline
#SBATCH --partition=normal
#SBATCH --account=a145
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G

#SBATCH --output=/capstor/scratch/cscs/anastasiia_kucherenko/fineweb_indexing/test_scale/output/es_search_%j.out
#SBATCH --error=/capstor/scratch/cscs/anastasiia_kucherenko/fineweb_indexing/test_scale/err/es_search_%j.err
#SBATCH --environment=elastictest

# Elasticsearch Search Pipeline Script with Configurable Query Parameters
# Usage: ./search_pipeline.sh [path_to_data] [csv_file] [index_name]
# All parameters can be provided via command line or environment variables

# TO DO:
# EXPLAIN JAVA OPTS SETTINGS CONFIG (XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=32m -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap"
# Improve adaptive heap based on memory available
# EXPLAIN WHY NEEDED ON CLARIDEN configure proxy bypass
# look into Disable swapping completely ( alternative to sudo swapoff -a 2>/dev/null || true SUDO NOT AVAILABLE)
# check_index_exists


set -e

# =============================================================================
# QUERY CONFIGURATION PARAMETERS
# =============================================================================
# Query execution configuration - set to true/false to enable/disable query types
EXECUTE_MATCH_QUERY="${EXECUTE_MATCH_QUERY:-true}"
EXECUTE_MATCH_PHRASE_QUERY="${EXECUTE_MATCH_PHRASE_QUERY:-true}"
EXECUTE_TERM_QUERY_EXACT="${EXECUTE_TERM_QUERY_EXACT:-false}"
EXECUTE_WILDCARD_QUERY="${EXECUTE_WILDCARD_QUERY:-false}"
EXECUTE_FUZZY_QUERY="${EXECUTE_FUZZY_QUERY:-false}"
EXECUTE_BOOL_MUST_QUERY="${EXECUTE_BOOL_MUST_QUERY:-false}"

# Match query operator parameter
# Can be ["or"], ["and"], or ["and","or"] for both queries
MATCH_QUERY_OPERATOR="${MATCH_QUERY_OPERATOR:-[\"or\"]}"

# Match phrase query slop parameter
# Can be a single value like "0" or multiple values like "[0,1,2]"
# Multiple values will create separate queries with different slop values
MATCH_PHRASE_SLOP="${MATCH_PHRASE_SLOP:-[0]}"

# Boolean must query parameters
BOOL_MUST_OPERATOR="${BOOL_MUST_OPERATOR:-or}"  # "and" or "or", should leave by default to "or" (otherwise query becomes match phrase)
BOOL_MUST_MAX_WORDS="${BOOL_MUST_MAX_WORDS:-3}"  # Maximum words to use in bool "and" query
BOOL_MUST_MINIMUM_SHOULD_MATCH="${BOOL_MUST_MINIMUM_SHOULD_MATCH:-50%}"  # For "or" operator (e.g., "2" or "50%")

# =============================================================================
# ELASTICSEARCH CONFIGURATION
# =============================================================================
ES_HOST="${ES_HOST:-127.0.0.1}"
ES_PORT="${ES_PORT:-9200}"

# The path directory to the ElasticSearch index you're querying (EXPLAIN HOW DIR WORK MOUNTS)
# in our case, one path dir equals one and only one index
PATH_DATA="${PATH_DATA:-/iopsstor/scratch/cscs/anastasiia_kucherenko/es-data-757733}"

# The path to the .csv file containing the queries (terms / phrases)
CSV_FILE="${CSV_FILE:-/capstor/scratch/cscs/anastasiia_kucherenko/index_attempt_1/search_queries/first_search.csv}"

# Name of your index
INDEX_NAME="${INDEX_NAME:-fineweb_test_1}"


CSV_BASENAME=$(basename "$CSV_FILE" .txt)
OUTPUT_DIR="${OUTPUT_DIR:-/capstor/scratch/cscs/anastasiia_kucherenko/index_attempt_1/search_results/${INDEX_NAME}_${CSV_BASENAME}_${SLURM_JOB_ID}/}"

# Build Elasticsearch URL
ES_URL="http://${ES_HOST}:${ES_PORT}"


# Calculate heap based on available memory, but be conservative FIXXXXX
if [ "${SLURM_MEM_PER_NODE:-0}" -ge 32768 ]; then
    # 32GB+ available, use 30GB heap (conservative, leaves room for OS + caches)
    JAVA_HEAP="-Xms30g -Xmx30g"
else
    # Less than 32GB, use 8GB heap
    JAVA_HEAP="-Xms8g -Xmx8g"
fi

# explain other settings
JAVA_OPTS="$JAVA_HEAP -XX:+UseG1GC -XX:MaxGCPauseMillis=200"


# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' 

# Parse command line arguments (override environment variables if provided)
if [ $# -ge 1 ] && [ -n "$1" ]; then
    PATH_DATA="$1"
fi

if [ $# -ge 2 ] && [ -n "$2" ]; then
    CSV_FILE="$2"
fi

if [ $# -ge 3 ] && [ -n "$3" ]; then
    INDEX_NAME="$3"
fi

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}


# Function to build configuration JSON for search function
build_config_json() {
    # Create temporary file for JSON construction
    local temp_json=$(mktemp)
    
    # Build JSON using proper escaping and formatting
    cat > "$temp_json" << EOF
{
    "execute_match_query": $EXECUTE_MATCH_QUERY,
    "execute_match_phrase_query": $EXECUTE_MATCH_PHRASE_QUERY,
    "execute_term_query_exact": $EXECUTE_TERM_QUERY_EXACT,
    "execute_wildcard_query": $EXECUTE_WILDCARD_QUERY,
    "execute_fuzzy_query": $EXECUTE_FUZZY_QUERY,
    "execute_bool_must_query": $EXECUTE_BOOL_MUST_QUERY,
    "match_query_operator": $MATCH_QUERY_OPERATOR,
    "match_phrase_slop": $MATCH_PHRASE_SLOP,
    "bool_must_operator": "$BOOL_MUST_OPERATOR",
    "bool_must_max_words": $BOOL_MUST_MAX_WORDS
EOF

    # Add minimum_should_match only if not empty
    if [ -n "$BOOL_MUST_MINIMUM_SHOULD_MATCH" ]; then
        # Check if it's a number or percentage
        if [[ "$BOOL_MUST_MINIMUM_SHOULD_MATCH" =~ ^[0-9]+%?$ ]]; then
            if [[ "$BOOL_MUST_MINIMUM_SHOULD_MATCH" =~ % ]]; then
                echo "    ,\"bool_must_minimum_should_match\": \"$BOOL_MUST_MINIMUM_SHOULD_MATCH\"" >> "$temp_json"
            else
                echo "    ,\"bool_must_minimum_should_match\": $BOOL_MUST_MINIMUM_SHOULD_MATCH" >> "$temp_json"
            fi
        else
            echo "    ,\"bool_must_minimum_should_match\": \"$BOOL_MUST_MINIMUM_SHOULD_MATCH\"" >> "$temp_json"
        fi
    fi
    
    echo "}" >> "$temp_json"
    
    # Validate JSON and output
    if command -v python3 >/dev/null 2>&1; then
        # Validate and minify JSON using Python
        local json_content=$(python3 -c "import json, sys; print(json.dumps(json.load(open('$temp_json'))))" 2>/dev/null)
        if [ $? -eq 0 ]; then
            echo "$json_content"
        else
            # Fallback: output raw content if Python validation fails
            cat "$temp_json"
        fi
    else
        # Fallback: output raw content if Python not available
        cat "$temp_json"
    fi
    
    # Cleanup
    rm -f "$temp_json"
}

configure_proxy_bypass() {
    log_info "Configuring proxy bypass for localhost connections..."
    
    # Save original proxy settings
    ORIGINAL_HTTP_PROXY="${http_proxy:-}"
    ORIGINAL_NO_PROXY="${no_proxy:-}"
    
    # Extend no_proxy to include all localhost variants
    export no_proxy="${no_proxy},127.0.0.1,localhost,0.0.0.0,::1"
    
    log_info "Original no_proxy: $ORIGINAL_NO_PROXY"
    log_info "Updated no_proxy: $no_proxy"
    log_info "HTTP proxy: ${http_proxy:-'(not set)'}"
}

# Validate required parameters
if [ -z "$PATH_DATA" ]; then
    log_error "PATH_DATA is required"
    log_info ""
    log_info "Usage: $0 [path_to_data] [csv_file] [index_name]"
    log_info ""
    log_info "You can provide parameters via:"
    log_info "1. Command line: $0 /path/to/es-data segments.csv my_index"
    log_info "2. Environment variables: PATH_DATA=/path/to/es-data CSV_FILE=segments.csv $0"
    log_info "3. Mix: PATH_DATA=/path/to/es-data $0 '' segments.csv my_index"
    log_info ""
    log_info "Environment variables (with defaults):"
    log_info "  PATH_DATA=<required>                    # Path to Elasticsearch data directory"
    log_info "  CSV_FILE=<required>                     # Path to CSV file with segments"
    log_info "  INDEX_NAME=default_index                # Elasticsearch index name"
    log_info "  ES_HOST=127.0.0.1                       # Elasticsearch host"
    log_info "  ES_PORT=9200                            # Elasticsearch port"
    log_info "  JAVA_HEAP_SIZE=nog                       # Java heap size for Elasticsearch"
    log_info ""
    log_info "Query Configuration (with defaults):"
    log_info "  EXECUTE_MATCH_QUERY=true                # Enable match query"
    log_info "  EXECUTE_MATCH_PHRASE_QUERY=true         # Enable match phrase query"
    log_info "  EXECUTE_TERM_QUERY_EXACT=false          # Enable term query (single words only)"
    log_info "  EXECUTE_WILDCARD_QUERY=false            # Enable wildcard query (single words only)"
    log_info "  EXECUTE_FUZZY_QUERY=true                # Enable fuzzy query"
    log_info "  EXECUTE_BOOL_MUST_QUERY=false           # Enable boolean must query"
    log_info "  MATCH_PHRASE_SLOP=[0]                   # Slop values for match phrase (e.g., [0,1,2])"
    log_info "  BOOL_MUST_OPERATOR=and                  # Boolean operator: 'and' or 'or'"
    log_info "  BOOL_MUST_MAX_WORDS=3                   # Max words for boolean query"
    log_info "  BOOL_MUST_MINIMUM_SHOULD_MATCH=         # Minimum should match (for 'or' operator)"
    exit 1
fi

if [ -z "$CSV_FILE" ]; then
    log_error "CSV_FILE is required"
    log_info "See usage above for details"
    exit 1
fi

# Check if CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    log_error "CSV file '$CSV_FILE' not found"
    exit 1
fi

# Check if data path exists
if [ ! -d "$PATH_DATA" ]; then
    log_error "Data path '$PATH_DATA' not found"
    exit 1
fi

log_info "Starting Elasticsearch Search Pipeline with Configurable Queries..."
log_info "========================================================================="
log_info "Configuration:"
log_info "  Data path: $PATH_DATA"
log_info "  CSV file: $CSV_FILE" 
log_info "  Index name: $INDEX_NAME"
log_info "  Elasticsearch: $ES_URL"
log_info "  Java heap: $JAVA_OPTS"
log_info "  Output directory: $OUTPUT_DIR"
log_info ""
log_info "Query Configuration:"
log_info "  Match Query: $EXECUTE_MATCH_QUERY"
log_info "  Match Query Operator: $MATCH_QUERY_OPERATOR"
log_info "  Match Phrase Query: $EXECUTE_MATCH_PHRASE_QUERY"
log_info "  Match Phrase Slop: $MATCH_PHRASE_SLOP"
log_info "  Term Query Exact: $EXECUTE_TERM_QUERY_EXACT"
log_info "  Wildcard Query: $EXECUTE_WILDCARD_QUERY"
log_info "  Fuzzy Query: $EXECUTE_FUZZY_QUERY"
log_info "  Bool Must Query: $EXECUTE_BOOL_MUST_QUERY"
log_info "  Bool Must Operator: $BOOL_MUST_OPERATOR"
log_info "  Bool Must Max Words: $BOOL_MUST_MAX_WORDS"
log_info "  Bool Must Min Should Match: ${BOOL_MUST_MINIMUM_SHOULD_MATCH:-'(not set)'}"
log_info "========================================================================="

# Elasticsearch optimizations for large index
start_elasticsearch() {
    log_info "Starting Elasticsearch with large index optimizations..."
     
    # Set environment variables
    unset JAVA_HOME
    export ES_JAVA_HOME="/usr/share/elasticsearch/jdk"

    # Test Java first
    log_info "Testing Java installation..."
    if $ES_JAVA_HOME/bin/java -version; then
        log_success "Java test successful"
    else
        log_error "Java test failed"
        return 1
    fi
    
    log_info "Using heap settings: $JAVA_OPTS"

    # Create a job logs directory
    local job_logs_dir="/iopsstor/scratch/cscs/anastasiia_kucherenko/es-search-logs-${SLURM_JOB_ID:-$$}"
    mkdir -p "$job_logs_dir"

    # Set the data directory to your indexed data directory (to be able to access it)
    local job_data_dir="$PATH_DATA" 

    log_info "=== Starting Elasticsearch ==="

   ES_JAVA_OPTS="$JAVA_OPTS" \
    /usr/share/elasticsearch/bin/elasticsearch \
        -E path.data="$job_data_dir" \
        -E path.logs="$job_logs_dir" \
        -E discovery.type=single-node \
        -E network.host=127.0.0.1 \
        -E http.host=127.0.0.1 \
        -E http.port=9200 \
        -E transport.host=127.0.0.1 \
        -E network.bind_host=127.0.0.1 \
        -E network.publish_host=127.0.0.1 \
        -E node.store.allow_mmap=false \
        -E xpack.security.enabled=false \
        -E cluster.routing.allocation.disk.watermark.low=85% \
        -E cluster.routing.allocation.disk.watermark.high=90% \
        -E cluster.routing.allocation.disk.watermark.flood_stage=95% \
        -E bootstrap.memory_lock=false \
        -E logger.root=INFO \
        -E http.max_content_length=200mb \
        > "$job_logs_dir/elasticsearch.out" 2>&1 &
    
    ES_PID=$!
    log_info "Elasticsearch started with PID: $ES_PID (optimized for 400GB index)"
    
    # Extended wait time for large index startup
    log_info "Waiting for large index to load (this may take several minutes)..."
    max_retries=120  
    retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        if ! kill -0 $ES_PID 2>/dev/null; then
            log_error "Elasticsearch process died! PID $ES_PID is no longer running"
            return 1
        fi
        
        if curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health" > /dev/null 2>&1; then
            log_success "Elasticsearch is ready!"
            
            # Show memory and performance stats
            log_info "=== Cluster Health ==="
            curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_cluster/health?pretty" 2>/dev/null
            
            log_info "=== Node Stats (Memory) ==="
            curl --noproxy "127.0.0.1" -s "http://127.0.0.1:9200/_nodes/stats/jvm,indices?pretty" 2>/dev/null | head -50
            
            return 0
        else
            retry_count=$((retry_count + 1))
            if [ $((retry_count % 10)) -eq 0 ]; then
                log_info "Still waiting for large index to load... attempt $retry_count/$max_retries"
                log_info "This is normal for such large indices - please be patient"
            fi
            sleep 10
        fi
    done
    
    log_error "Elasticsearch failed to start after $max_retries attempts"
    return 1
}

# Function to check if index exists
check_index_exists() {
    local index_name="$1"
    local http_code
    
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "$ES_URL/$index_name" 2>/dev/null)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -ne 0 ]; then
        log_warn "Failed to check index existence (curl error)"
        return 1
    fi
    
    case "$http_code" in
        200)
            return 0
            ;;
        404)
            return 1
            ;;
        *)
            log_warn "Unexpected HTTP code when checking index: $http_code"
            return 1
            ;;
    esac
}

# Function to list available indices with better error handling
list_indices() {
    log_info "Available indices:"
    
    # Try simple format first (most reliable)
    local response
    response=$(curl -s "$ES_URL/_cat/indices?v" 2>&1)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -eq 0 ] && [ -n "$response" ]; then
        # Check if we got HTML instead of expected output
        if echo "$response" | grep -qi "<!DOCTYPE\|<html"; then
            log_warn "Received HTML response instead of indices list"
            log_warn "This usually means Elasticsearch returned an error page"
            log_info "Trying JSON format..."
            
            # Try JSON format as fallback
            local json_response
            json_response=$(curl -s "$ES_URL/_cat/indices?format=json" 2>&1)
            if [ $? -eq 0 ] && echo "$json_response" | grep -q "^\["; then
                echo "$json_response"
            else
                log_error "Both formats failed. Raw response:"
                echo "$response" | head -10
                return 1
            fi
        else
            # Normal response, show it
            echo "$response"
        fi
    else
        log_error "Failed to fetch indices (exit code: $curl_exit_code)"
        if [ -n "$response" ]; then
            log_error "Response: $response"
        fi
        return 1
    fi
}

# Function to cleanup on exit
cleanup() {
    if [ ! -z "$ES_PID" ] && kill -0 $ES_PID 2>/dev/null; then
        log_info "Stopping Elasticsearch (PID: $ES_PID)..."
        kill $ES_PID
        wait $ES_PID 2>/dev/null || true
    fi
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Main execution function
main() {
    configure_proxy_bypass

    # Start Elasticsearch
    if ! start_elasticsearch; then
        log_error "Failed to start Elasticsearch"
        exit 1
    fi
    
    # Test connection after proxy fix
    log_info "=== Testing connection after proxy fix ==="
    log_info "Testing root endpoint without proxy:"
    curl -v "http://127.0.0.1:9200/" 2>&1 | head -10
    log_info "=== End connection test ==="
    
    # List available indices with better error handling
    log_info "Checking available indices..."
    if ! list_indices; then
        log_error "Failed to list indices, but continuing anyway..."
        log_info "You may need to check the index name manually"
    fi
    
    # Check if specified index exists
    log_info "Checking if index '$INDEX_NAME' exists..."
    if ! check_index_exists "$INDEX_NAME"; then
        log_warn ""
        log_warn "Index '$INDEX_NAME' not found or couldn't verify!"
        log_info "Attempting to list indices again..."
        list_indices
        log_info ""
        log_info "If the index exists but isn't showing, there might be a connectivity issue."
        log_info "The search pipeline will attempt to continue anyway."
        log_warn "If searches fail, verify the index name and Elasticsearch health."
    else
        log_success ""
        log_success "Index '$INDEX_NAME' found. Proceeding with search pipeline..."
    fi
    
    # Build configuration JSON 
    log_info "Building query configuration..."
    CONFIG_JSON=$(build_config_json)
    
    # Validate the JSON was created properly
    if [ -z "$CONFIG_JSON" ]; then
        log_error "Failed to build configuration JSON"
        exit 1
    fi
    
    
    # Run Python search script with configuration
    log_info "Starting search queries execution with configurable parameters..."
    if python3 /capstor/scratch/cscs/anastasiia_kucherenko/index_attempt_1/my_search.py "$CSV_FILE" "$INDEX_NAME" "$ES_URL" "$OUTPUT_DIR" "$CONFIG_JSON"; then
        log_success ""
        log_success "Search pipeline completed successfully!"
        log_info "Output files saved to: $OUTPUT_DIR"
        log_info "Check the output files for detailed results and statistics."
        log_info ""
        log_info "Query configuration used:"
        echo "$CONFIG_JSON" | python3 -m json.tool 2>/dev/null || echo "$CONFIG_JSON"
    else
        log_error "Search pipeline failed!"
        exit 1
    fi
}

# Run main function
main
