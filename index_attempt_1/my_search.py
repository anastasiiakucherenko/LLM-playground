#!/usr/bin/env python3
"""
Elasticsearch Search Queries Pipeline
Performs various search query types and 
Enhanced with hit snippets for better analysis
Configurable query execution parameters
"""

import csv
import json
import time
import requests
import sys
import re 
import statistics
from datetime import datetime
from typing import Dict, List, Tuple, Any, Union
import ast

# TO DO:
# REMOVE WILDCARD QUERY?

class ElasticsearchQueryBenchmark:
    def __init__(self, es_url: str, index_name: str, config: Dict[str, Any] = None):
        self.es_url = es_url.rstrip('/')
        self.index_name = index_name
        self.results = []

        # Optimizations for large index (large timemout)
        self.request_timeout = 60  
        self.max_retries = 3
        
        # Default configuration
        default_config = {
            'execute_match_query': True,
            'execute_match_phrase_query': True,
            'execute_term_query_exact': False,
            'execute_wildcard_query': False,
            'execute_fuzzy_query': True,
            'execute_bool_must_query': False,
            'match_query_operator': ['or'],  # ['and'], ['or'], or ['and', 'or']
            'match_phrase_slop': [0],  # Can be single value or list
            'bool_must_operator': 'and',  # 'and' or 'or'
            'bool_must_max_words': 3,  # Maximum words to use in bool query
            'bool_must_minimum_should_match': None  # For 'or' operator
        }
        
        # Update with provided config
        if config:
            default_config.update(config)
        
        # Set configuration as instance variables
        for key, value in default_config.items():
            setattr(self, key, value)
        
        # Ensure match_phrase_slop is a list
        if not isinstance(self.match_phrase_slop, list):
            self.match_phrase_slop = [self.match_phrase_slop]
            
        # Ensure match_query_operator is a list
        if not isinstance(self.match_query_operator, list):
            self.match_query_operator = [self.match_query_operator]
        
        # Update query types to include variations
        self.query_types = [
            'match_query',
            'match_phrase_query', 
            'term_query_exact',
            'wildcard_query',
            'fuzzy_query',
            'bool_must_query'
        ]
        
        # Add match query operator variations if multiple operators
        if len(self.match_query_operator) > 1:
            self.query_types.remove('match_query')
            for operator in self.match_query_operator:
                self.query_types.append(f'match_query_{operator}')
        elif len(self.match_query_operator) == 1 and self.match_query_operator[0] != 'or':
            self.query_types.remove('match_query')
            self.query_types.append(f'match_query_{self.match_query_operator[0]}')
        
        # Add slop variations to query types if multiple slop values
        if len(self.match_phrase_slop) > 1:
            self.query_types.remove('match_phrase_query')
            for slop in self.match_phrase_slop:
                self.query_types.append(f'match_phrase_query_slop_{slop}')
        elif len(self.match_phrase_slop) == 1 and self.match_phrase_slop[0] != 0:
            self.query_types.remove('match_phrase_query')
            self.query_types.append(f'match_phrase_query_slop_{self.match_phrase_slop[0]}')

    def _is_single_word(self, text: str) -> bool:
        """
        Check if the text contains only a single word (no spaces, punctuation creates separate tokens)
        """
        # Remove punctuation and split into words
        words = re.findall(r'\b\w+\b', text.strip())
        return len(words) == 1
         
    def _make_request(self, method: str, endpoint: str, data: dict = None) -> Tuple[dict, float]:
        """Make HTTP request to Elasticsearch and measure response time"""
        url = f"{self.es_url}/{endpoint}"
        headers = {
            'Content-Type': 'application/json',
            'Connection': 'keep-alive'  # Reuse connections
        }
        
        for attempt in range(self.max_retries):
            start_time = time.time()
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=headers, timeout=self.request_timeout)
                elif method.upper() == 'POST':
                    response = requests.post(url, headers=headers, json=data, timeout=self.request_timeout)
                
                end_time = time.time()
                query_time = (end_time - start_time) * 1000
                
                response.raise_for_status()
                return response.json(), query_time
                
            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    print(f"    Timeout on attempt {attempt + 1}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    end_time = time.time()
                    query_time = (end_time - start_time) * 1000
                    return {"error": "Timeout after retries"}, query_time
                    
            except requests.exceptions.RequestException as e:
                end_time = time.time()
                query_time = (end_time - start_time) * 1000
                print(f"Request failed: {e}")
                return {"error": str(e)}, query_time

    def match_query(self, text: str, operator: str = 'or') -> Tuple[dict, float]:
        """Standard match query using the main text field with configurable operator"""
        match_config = {
            "query": text
        }
        
        # Add operator if not default 'or'
        if operator.lower() != 'or':
            match_config["operator"] = operator.lower()
        
        query = {
            "query": {
                "match": {
                    "text": match_config
                }
            },
            "size": 50,
            "_source": ["url"],  # Don't return full text source, just highlights, and yes url
            "highlight": {
                "fields": {
                    "text": {
                        "fragment_size": 150,
                        "number_of_fragments": 3,
                        "pre_tags": ["<MATCH>"],
                        "post_tags": ["</MATCH>"],
                        "require_field_match": True
                    }
                }
            },
            "timeout": "30s"  # Query timeout
        }
        return self._make_request('POST', f"{self.index_name}/_search", query)
    
    def match_phrase_query(self, text: str, slop: int = 0) -> Tuple[dict, float]:
        """Match phrase query for exact phrase matching with configurable slop"""
        match_phrase_config = {
            "query": text
        }
        
        # Add slop parameter if not 0
        if slop > 0:
            match_phrase_config["slop"] = slop
        
        query = {
            "query": {
                "match_phrase": {
                    "text": match_phrase_config  # If set to text.exact, unable to retrieve highlights on text field or text.exact field. set to text and correct highlights
                }
            },
            "size": 50,
            "_source": ["url"],
            "highlight": {
                "fields": {
                    "text": {
                        "fragment_size": 150,
                        "number_of_fragments": 3,
                        "pre_tags": ["<MATCH>"],
                        "post_tags": ["</MATCH>"],
                        "require_field_match": True
                    }
                }
            },
            "timeout": "30s"  # Query timeout
        }
        return self._make_request('POST', f"{self.index_name}/_search", query)
        
    def term_query_exact(self, text: str) -> Tuple[dict, float]:
        """Term query - ONLY executes on single words, returns empty result for multi-word"""
        
        if not self._is_single_word(text):
            print(f"    SKIPPING term_query_exact for multi-word text: '{text[:50]}...'")
            print(f"    Returning empty result (no fallback)")
            # Return empty result structure
            return {
                "hits": {"total": {"value": 0}, "max_score": None, "hits": []},
                "took": 0,
                "timed_out": False
            }, 0.0
        else:
            print(f"    Executing term_query_exact on single word: '{text}'")
            query = {
                "query": {
                    "term": {
                        "text.exact": text.lower()
                    }
                },
                "size": 100,
                "_source": ["url"],
                "highlight": {
                    "fields": {
                        "text": {
                            "fragment_size": 200,
                            "number_of_fragments": 5,
                            "pre_tags": ["<MATCH>"],
                            "post_tags": ["</MATCH>"],
                            "require_field_match": True
                        }
                    }
                }
            }
            
            return self._make_request('POST', f"{self.index_name}/_search", query)
      
    def wildcard_query(self, text: str) -> Tuple[dict, float]:
        """Wildcard query - ONLY executes on single words, returns empty result for multi-word"""
        
        if not self._is_single_word(text):
            print(f"    SKIPPING wildcard_query for multi-word text: '{text[:50]}...'")
            print(f"    Returning empty result (no fallback)")
            # Return empty result structure
            return {
                "hits": {"total": {"value": 0}, "max_score": None, "hits": []},
                "took": 0,
                "timed_out": False
            }, 0.0
        else:
            print(f"    Executing wildcard_query on single word: '{text}'")
            wildcard_text = f"*{text.lower()}*"
            query = {
                "query": {
                    "wildcard": {
                        "text.exact": wildcard_text
                    }
                },
                "size": 100,
                "highlight": {
                    "fields": {
                        "text": {
                            "fragment_size": 200,
                            "number_of_fragments": 5,
                            "pre_tags": ["<MATCH>"],
                            "post_tags": ["</MATCH>"],
                            "require_field_match": True
                        }
                    }
                }
            }
            
            return self._make_request('POST', f"{self.index_name}/_search", query)
     

    def fuzzy_query(self, text: str) -> Tuple[dict, float]:
        """Fuzzy query - ONLY executes on single words, uses multi_match fallback for multi-word"""
        
        if not self._is_single_word(text):
            words = text.split()
            query_text = " ".join(words)
            print(f"    SKIPPING fuzzy_query for multi-word text: '{text[:50]}...'")
            print(f"    Using multi_match with fuzziness fallback instead")
            if word_count <= 2:
                min_should_match = "100%"    # Short queries: all words
            elif word_count <= 4: 
                min_should_match = "75%"     # Medium queries: most words
            else:
                min_should_match = "60%"     # Long queries: majority of words
            
           
            query = {
                "query": {
                    "multi_match": {
                        "query": query_text,
                        "fields": ["text"],
                        "fuzziness": "AUTO",  
                        "operator": "or",
                        "max_expansions": 50,  # Limit term expansions (can be too much pressure)
                        "minimum_should_match": min_should_match
                    }
                },
                "size": 50,
                "_source": ["url"],
                "highlight": {
                    "fields": {
                        "text": {
                            "fragment_size": 150,
                            "number_of_fragments": 3,
                            "pre_tags": ["<MATCH>"],
                            "post_tags": ["</MATCH>"],
                            "require_field_match": True
                        }
                    }
                },
                "timeout": "30s"
            }
        else:
            print(f"    Executing fuzzy_query on single word: '{text}'")
            query = {
                "query": {
                    "fuzzy": {
                        "text": {
                            "value": text,
                            "fuzziness": "AUTO"
                        }
                    }
                },
                "size": 50,
                "_source": ["url"],
                "highlight": {
                    "fields": {
                        "text": {
                            "fragment_size": 150,
                            "number_of_fragments": 3,
                            "pre_tags": ["<MATCH>"],
                            "post_tags": ["</MATCH>"],
                            "require_field_match": True
                        }
                    }
                }
            }
        return self._make_request('POST', f"{self.index_name}/_search", query)
        
    
    def bool_must_query(self, text: str) -> Tuple[dict, float]:
        """Boolean query with configurable parameters"""
        if self.bool_must_operator.lower() == 'and':
            # For AND: limit words to avoid too complex queries
            words = text.split()[:self.bool_must_max_words]
            if len(words) < 2:
                words = [text, text]  # Duplicate if single word
            
            must_clauses = [{"match": {"text": word}} for word in words]
            # fallsback to a simple match prhase query, not FUN
            # see what more complex stuff you can do
            query = {
                "query": {
                    "bool": {
                        "must": must_clauses
                    }
                },
                "size": 50,
                "_source": ["url"],
                "highlight": {
                    "fields": {
                        "text": {
                            "fragment_size": 150,
                            "number_of_fragments": 3,
                            "pre_tags": ["<MATCH>"],
                            "post_tags": ["</MATCH>"],
                            "require_field_match": True
                        }
                    }
                }
            }
        else:
            # For OR: use all words, don't limit upfront
            words = text.split()
            if len(words) < 2:
                words = [text, text]  # Duplicate if single word
            
            should_clauses = [{"match": {"text": word}} for word in words]
            
            bool_query = {
                "should": should_clauses
            }
            
            # Add minimum_should_match if specified
            if self.bool_must_minimum_should_match is not None:
                bool_query["minimum_should_match"] = self.bool_must_minimum_should_match
            
            query = {
                "query": {
                    "bool": bool_query
                },
                "size": 50,
                "_source": ["url"],
                "highlight": {
                    "fields": {
                        "text": {
                            "fragment_size": 150,
                            "number_of_fragments": 3,
                            "pre_tags": ["<MATCH>"],
                            "post_tags": ["</MATCH>"],
                            "require_field_match": True
                        }
                    }
                }
            }
            
        return self._make_request('POST', f"{self.index_name}/_search", query)
    
    def extract_hit_snippets(self, hits_data: list, max_hits: int = 5) -> str:
        """Extract top N hit snippets with scores and highlighting"""
        if not hits_data:
            return ""
        
        snippets = []
        for i, hit in enumerate(hits_data[:max_hits]):
            score = hit.get('_score', 0)

            # Extract URL from source
            url = hit.get('_source', {}).get('url', 'No URL available')
        
            
            # Get highlighted text if available (this contains the matching terms)
            if 'highlight' in hit and 'text' in hit['highlight']:
                # Use highlighted fragments that contain the query terms
                highlighted_fragments = hit['highlight']['text']
                text_snippet = ' | '.join(highlighted_fragments)
                snippet_source = "HIGHLIGHTED"
            else:
                # Fallback to source text if highlighting failed
                source_text = hit.get('_source', {}).get('text', '')
                # For fallback, try to find query terms in the text
                text_snippet = source_text[:300] + ('...' if len(source_text) > 300 else '')
                snippet_source = "SOURCE_TEXT"
            
            # Clean up the snippet (remove extra whitespace and newlines)
            text_snippet = ' '.join(text_snippet.split())
            
            # Include URL in snippet info
            snippet_info = f"Hit {i+1} (Score: {score:.3f}, URL: {url}, Type: {snippet_source}): {text_snippet}"
            snippets.append(snippet_info)
        
        return '\n'.join(snippets)
    
    def extract_response_stats(self, response: dict) -> dict:
        """Extract relevant statistics from ES response"""
        if "error" in response:
            return {
                "total_hits": 0,
                "max_score": 0,
                "took_ms": 0,
                "timed_out": False,
                "error": response["error"],
                "hit_snippets": ""
            }
        
        hits = response.get("hits", {})
        hits_data = hits.get("hits", [])
        
        return {
            "total_hits": hits.get("total", {}).get("value", 0) if isinstance(hits.get("total"), dict) else hits.get("total", 0),
            "max_score": hits.get("max_score", 0) or 0,
            "took_ms": response.get("took", 0),
            "timed_out": response.get("timed_out", False),
            "error": None,
            "hit_snippets": self.extract_hit_snippets(hits_data)
        }
    
    def run_all_queries(self, segment_text: str, row_id: str, segment_id: str) -> List[dict]:
        """Run all enabled query types for a given segment text"""
        query_results = []
        
        # Build query methods based on configuration
        query_methods = {}
        
        if self.execute_match_query:
            # Handle multiple operators
            for operator in self.match_query_operator:
                if operator == 'or' and len(self.match_query_operator) == 1:
                    query_name = 'match_query'
                else:
                    query_name = f'match_query_{operator}'
                query_methods[query_name] = lambda text, op=operator: self.match_query(text, op)
            
        if self.execute_match_phrase_query:
            # Handle multiple slop values
            for slop in self.match_phrase_slop:
                if slop == 0:
                    query_name = 'match_phrase_query'
                else:
                    query_name = f'match_phrase_query_slop_{slop}'
                query_methods[query_name] = lambda text, s=slop: self.match_phrase_query(text, s)
                
        if self.execute_term_query_exact:
            query_methods['term_query_exact'] = lambda text: self.term_query_exact(text)
            
        if self.execute_wildcard_query:
            query_methods['wildcard_query'] = lambda text: self.wildcard_query(text)
            
        if self.execute_fuzzy_query:
            query_methods['fuzzy_query'] = lambda text: self.fuzzy_query(text)
            
        if self.execute_bool_must_query:
            query_methods['bool_must_query'] = lambda text: self.bool_must_query(text)
        
        for query_type, method in query_methods.items():
            print(f"  Running {query_type}...")
            
            try:
                response, query_time = method(segment_text)
                stats = self.extract_response_stats(response)
                
                result = {
                    'timestamp': datetime.now().isoformat(),
                    'row_id': row_id,
                    'segment_id': segment_id,
                    'segment_text': segment_text,
                    'query_type': query_type,
                    'query_time_ms': round(query_time, 2),
                    'es_took_ms': stats['took_ms'],
                    'total_hits': stats['total_hits'],
                    'max_score': stats['max_score'],
                    'timed_out': stats['timed_out'],
                    'error': stats['error'],
                    'top_5_hits': stats['hit_snippets']
                }
                
                query_results.append(result)
                self.results.append(result)
                
            except Exception as e:
                print(f"    Error in {query_type}: {e}")
                error_result = {
                    'timestamp': datetime.now().isoformat(),
                    'row_id': row_id,
                    'segment_id': segment_id,
                    'segment_text': segment_text,
                    'query_type': query_type,
                    'query_time_ms': 0,
                    'es_took_ms': 0,
                    'total_hits': 0,
                    'max_score': 0,
                    'timed_out': False,
                    'error': str(e),
                    'top_5_hits': ''
                }
                query_results.append(error_result)
                self.results.append(error_result)
        
        return query_results
    
    def process_csv(self, csv_file: str):
        """Process the CSV file and run queries for each segment"""
        print(f"Processing CSV file: {csv_file}")
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                total_rows = 0
                processed_rows = 0
                
                # Count total rows first
                for row in reader:
                    total_rows += 1
                
                # Reset file pointer
                file.seek(0)
                reader = csv.DictReader(file)
                
                print(f"Found {total_rows} segments to process")
                print("=" * 50)
                
                for row in reader:
                    processed_rows += 1
                    segment_text = row.get('segment_text', '').strip()
                    row_id = row.get('row_id', '')
                    segment_id = row.get('segment_id', '')
                    
                    if not segment_text:
                        print(f"Skipping empty segment at row {processed_rows}")
                        continue
                    
                    print(f"Processing segment {processed_rows}/{total_rows}: '{segment_text[:50]}{'...' if len(segment_text) > 50 else ''}'")
                    
                    self.run_all_queries(segment_text, row_id, segment_id)
                    
                    # Progress indicator
                    if processed_rows % 10 == 0:
                        print(f"Progress: {processed_rows}/{total_rows} segments processed")
                        
        except FileNotFoundError:
            print(f"Error: CSV file '{csv_file}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error processing CSV file: {e}")
            sys.exit(1)
    
    def save_detailed_results(self, filename: str = 'search_results_detailed.csv'):
        """Save detailed results to CSV file with enhanced formatting"""
        print(f"Saving detailed results to {filename}...")
        
        fieldnames = [
            'timestamp', 'row_id', 'segment_id', 'segment_text', 'query_type',
            'query_time_ms', 'es_took_ms', 'total_hits', 'max_score', 
            'timed_out', 'error', 'top_5_hits'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            # Group results by segment for better readability
            current_segment = None
            for result in self.results:
                # Add separator row between different segments
                if current_segment != result['segment_id'] and current_segment is not None:
                    separator_row = {field: '---' if field != 'top_5_hits' else '' for field in fieldnames}
                    separator_row['segment_text'] = f"--- END SEGMENT {current_segment} ---"
                    writer.writerow(separator_row)
                
                writer.writerow(result)
                current_segment = result['segment_id']
        
        print(f"Detailed results saved to {filename}")
        print(f"Results include top 5 hit snippets with scores and highlighting")
    
    def generate_summary_stats(self, filename: str = 'search_results_summary.csv'):
        """Generate and save summary statistics"""
        print(f"Generating summary statistics...")
        
        if not self.results:
            print("No results to analyze")
            return
        
        # Group results by query type
        stats_by_type = {}
        total_queries = len(self.results)
        successful_queries = len([r for r in self.results if r['error'] is None])
        
        for query_type in self.query_types:
            type_results = [r for r in self.results if r['query_type'] == query_type and r['error'] is None]
            
            if type_results:
                query_times = [r['query_time_ms'] for r in type_results]
                es_times = [r['es_took_ms'] for r in type_results]
                hit_counts = [r['total_hits'] for r in type_results]
                
                stats_by_type[query_type] = {
                    'total_queries': len(type_results),
                    'avg_query_time_ms': round(statistics.mean(query_times), 2),
                    'median_query_time_ms': round(statistics.median(query_times), 2),
                    'min_query_time_ms': round(min(query_times), 2),
                    'max_query_time_ms': round(max(query_times), 2),
                    'avg_es_time_ms': round(statistics.mean(es_times), 2),
                    'avg_hits': round(statistics.mean(hit_counts), 2),
                    'total_hits': sum(hit_counts),
                    'errors': len([r for r in self.results if r['query_type'] == query_type and r['error'] is not None])
                }
            else:
                stats_by_type[query_type] = {
                    'total_queries': 0,
                    'avg_query_time_ms': 0,
                    'median_query_time_ms': 0,
                    'min_query_time_ms': 0,
                    'max_query_time_ms': 0,
                    'avg_es_time_ms': 0,
                    'avg_hits': 0,
                    'total_hits': 0,
                    'errors': len([r for r in self.results if r['query_type'] == query_type])
                }
        
        # Save summary statistics
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            fieldnames = [
                'query_type', 'total_queries', 'avg_query_time_ms', 'median_query_time_ms',
                'min_query_time_ms', 'max_query_time_ms', 'avg_es_time_ms',
                'avg_hits', 'total_hits', 'errors'
            ]
            
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            for query_type, stats in stats_by_type.items():
                row = {'query_type': query_type}
                row.update(stats)
                writer.writerow(row)
        
        # Print summary to console
        print("\n" + "=" * 60)
        print("SEARCH PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Total queries executed: {total_queries}")
        print(f"Successful queries: {successful_queries}")
        print(f"Failed queries: {total_queries - successful_queries}")
        print()
        
        print("Performance by Query Type:")
        print("-" * 40)
        for query_type, stats in stats_by_type.items():
            print(f"{query_type}:")
            print(f"  Queries: {stats['total_queries']}")
            print(f"  Avg Time: {stats['avg_query_time_ms']}ms")
            print(f"  Avg Hits: {stats['avg_hits']}")
            print(f"  Errors: {stats['errors']}")
            print()
        
        print(f"Summary statistics saved to {filename}")

def parse_config_value(value_str: str) -> Union[bool, int, str, List]:
    """Parse configuration values from string"""
    if not value_str:
        return None
        
    # Handle boolean values
    if value_str.lower() in ('true', 'false'):
        return value_str.lower() == 'true'
    
    # Handle lists (for slop values)
    if value_str.startswith('[') and value_str.endswith(']'):
        try:
            return ast.literal_eval(value_str)
        except:
            return [value_str]
    
    # Handle integers
    try:
        return int(value_str)
    except ValueError:
        pass
    
    # Return as string
    return value_str

def main():
    if len(sys.argv) < 5:
        print("Usage: python3 search_with_highlight.py <csv_file> <index_name> <es_url> <output_dir> [config_json]")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    index_name = sys.argv[2]
    es_url = sys.argv[3]
    output_dir = sys.argv[4]
    
    # Parse configuration if provided
    config = {}
    if len(sys.argv) > 5:
        try:
            config = json.loads(sys.argv[5])
        except json.JSONDecodeError as e:
            print(f"Error parsing configuration JSON: {e}")
            sys.exit(1)
    
    print("Elasticsearch Search Pipeline Starting...")
    print(f"CSV File: {csv_file}")
    print(f"Index: {index_name}")
    print(f"ES URL: {es_url}")
    print(f"Output Directory: {output_dir}")
    print(f"Configuration: {config}")
    print("=" * 50)
    
    # Create output directory if it doesn't exist
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize benchmark with configuration
    benchmark = ElasticsearchQueryBenchmark(es_url, index_name, config)
    
    # Test ES connection
    try:
        response, _ = benchmark._make_request('GET', '')
        print(f"Connected to Elasticsearch: {response.get('tagline', 'Unknown version')}")
    except Exception as e:
        print(f"Failed to connect to Elasticsearch: {e}")
        sys.exit(1)
    
    # Process CSV and run queries
    start_time = time.time()
    benchmark.process_csv(csv_file)
    end_time = time.time()
    
    total_time = end_time - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds")
    
    # Save results with timestamps for uniqueness
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_id = os.environ.get('SLURM_JOB_ID', 'local')
    
    detailed_filename = os.path.join(output_dir, f"search_results_detailed_{job_id}_{timestamp}.csv")
    summary_filename = os.path.join(output_dir, f"search_results_summary_{job_id}_{timestamp}.csv")
    
    benchmark.save_detailed_results(detailed_filename)
    benchmark.generate_summary_stats(summary_filename)
    
    print("\nPipeline completed successfully!")
    print(f"Enhanced results with hit snippets saved to: {detailed_filename}")

if __name__ == "__main__":
    main()