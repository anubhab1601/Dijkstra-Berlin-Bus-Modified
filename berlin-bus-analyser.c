#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <direct.h>
#include <errno.h>
#include <time.h>

// Constants for array sizes and infinity value
#define MAX_LINE_LENGTH 1024
#define MAX_ROUTES 2000
#define MAX_NODES 15000
#define INF 1000000000

// Structure for connections in a route (linked list of stops)
typedef struct RouteConnection {
    int from_stop;            // Starting stop ID
    int to_stop;              // Ending stop ID
    float duration;           // Travel time between stops
    struct RouteConnection* next; // Pointer to next connection
} RouteConnection;

// Structure for a route (linked list of routes)
typedef struct Route {
    int route_id;             // Unique route ID
    RouteConnection* connections; // List of connections in this route
    struct Route* next;       // Pointer to next route
} Route;

// Structure to store Dijkstra's algorithm performance statistics
typedef struct {
    double exec_time;         // Execution time in milliseconds
    int comparisons;          // Number of distance comparisons
    int relaxations;          // Number of edge relaxations
    int parent_changes;       // Number of parent node updates
    size_t memory_consumed;   // Memory used by edges
    size_t auxiliary_size;    // Memory used by auxiliary arrays
} DijkstraStats;

// Structure for an edge in the adjacency list
typedef struct Edge {
    int dest;                 // Destination node ID
    float weight;             // Edge weight (travel time)
    struct Edge* next;        // Pointer to next edge
} Edge;

// Structure for a node in the graph
typedef struct {
    int node;                 // Node ID
} Node;

// Global variables
Route* route_map[MAX_ROUTES] = {NULL}; // Hash table for routes
Edge* adj[MAX_NODES];                 // Adjacency list for graph
float dist[MAX_NODES];                // Shortest path distances
int parent[MAX_NODES];                // Parent nodes in shortest paths
float segment_time[MAX_NODES];        // Time for each segment
Node nodes[MAX_NODES];                // List of unique nodes
int node_count = 0;                   // Number of unique nodes
DijkstraStats stats;                  // Performance statistics

// Function to get or create a route in the hash table
Route* get_route(int route_id) {
    int index = route_id % MAX_ROUTES; // Hash route_id to index
    Route* current = route_map[index];
    while (current) {
        if (current->route_id == route_id) return current; // Route found
        current = current->next;
    }
    
    // Create new route if not found
    Route* new_route = (Route*)malloc(sizeof(Route));
    new_route->route_id = route_id;
    new_route->connections = NULL;
    new_route->next = route_map[index];
    route_map[index] = new_route;
    return new_route;
}

// Function to add a connection to a route
void add_route_connection(Route* route, int from_stop, int to_stop, float duration) {
    // Allocate memory for new connection
    RouteConnection* new_conn = (RouteConnection*)malloc(sizeof(RouteConnection));
    new_conn->from_stop = from_stop;
    new_conn->to_stop = to_stop;
    new_conn->duration = duration;
    new_conn->next = NULL;

    // Add connection to route's connection list
    if (!route->connections) {
        route->connections = new_conn;
    } else {
        RouteConnection* last = route->connections;
        while (last->next) last = last->next;
        last->next = new_conn;
    }
}

// Function to free memory used by routes
void free_route_memory() {
    for (int i = 0; i < MAX_ROUTES; i++) {
        Route* route = route_map[i];
        while (route) {
            // Free all connections in the route
            RouteConnection* conn = route->connections;
            while (conn) {
                RouteConnection* temp = conn;
                conn = conn->next;
                free(temp);
            }
            // Free the route itself
            Route* temp = route;
            route = route->next;
            free(temp);
        }
        route_map[i] = NULL;
    }
}

// Function to build adjacency list from CSV file
void build_adjacency_list() {
    // Open input file
    FILE* input_file = fopen("network_temporal.csv", "r");
    if (!input_file) {
        perror("Error opening input file");
        exit(1);
    }

    // Open output file
    FILE* output_file = fopen("berlin_list_bus.csv", "w");
    if (!output_file) {
        perror("Error opening output file");
        fclose(input_file);
        exit(1);
    }

    char line[MAX_LINE_LENGTH];
    fgets(line, sizeof(line), input_file); // Skip header line

    // Read and parse each line
    while (fgets(line, sizeof(line), input_file)) {
        if (strlen(line) <= 1) continue; // Skip empty lines

        int from_stop, to_stop, dep_time, arr_time, route_type, trip_I, seq, route_I;
        // Parse CSV line
        if (sscanf(line, "%d,%d,%d,%d,%d,%d,%d,%d", 
                    &from_stop, &to_stop, &dep_time, &arr_time, 
                    &route_type, &trip_I, &seq, &route_I) != 8) {
            fprintf(stderr, "Failed to parse line: %s\n", line);
            continue;
        }

        // Calculate duration and add to route
        float duration = arr_time - dep_time;
        Route* route = get_route(route_I);
        add_route_connection(route, from_stop, to_stop, duration);
    }

    // Write routes to output file
    for (int i = 0; i < MAX_ROUTES; i++) {
        Route* route = route_map[i];
        while (route) {
            fprintf(output_file, "%d", route->route_id);
            RouteConnection* conn = route->connections;
            while (conn) {
                fprintf(output_file, ";[%d,%d,%.2f]", conn->from_stop, conn->to_stop, conn->duration);
                conn = conn->next;
            }
            fprintf(output_file, "\n");
            route = route->next;
        }
    }

    fclose(input_file);
    fclose(output_file);
}

// Function to add an edge to the adjacency list
void addEdge(int u, int v, float weight) {
    // Allocate memory for new edge
    Edge* edge = (Edge*)malloc(sizeof(Edge));
    edge->dest = v;
    edge->weight = weight;
    edge->next = adj[u];
    adj[u] = edge;
    stats.memory_consumed += sizeof(Edge); // Track memory usage
}

// Function to load graph from adjacency list file
void loadGraph(const char* filename) {
    // Open input file
    FILE* file = fopen(filename, "r");
    if (!file) {
        printf("Error: Could not find file '%s'\n", filename);
        printf("Current directory: %s\n", _getcwd(NULL, 0));
        exit(1);
    }
    
    char line[1024];
    // Read each line
    while (fgets(line, sizeof(line), file)) {
        char* token = strtok(line, ";"); // Skip route ID
        if (!token) continue;
        
        // Parse connections
        token = strtok(NULL, ";");
        while (token != NULL) {
            int from, to;
            float weight;
            if (sscanf(token, "[%d,%d,%f]", &from, &to, &weight) == 3) {
                addEdge(from, to, weight); // Add edge to graph
                // Track unique nodes
                int found_from = 0, found_to = 0;
                for (int i = 0; i < node_count; i++) {
                    if (nodes[i].node == from) found_from = 1;
                    if (nodes[i].node == to) found_to = 1;
                }
                if (!found_from) nodes[node_count++].node = from;
                if (!found_to) nodes[node_count++].node = to;
            }
            token = strtok(NULL, ";");
        }
    }
    fclose(file);
}

// Function to initialize performance statistics
void initStats() {
    stats.exec_time = 0;
    stats.comparisons = 0;
    stats.relaxations = 0;
    stats.parent_changes = 0;
    stats.memory_consumed = 0;
    stats.auxiliary_size = node_count * (sizeof(float) + sizeof(int) + sizeof(float));
}

// Function to run Dijkstra's algorithm from a source node
void dijkstra(int start) {
    initStats(); // Reset stats
    clock_t start_time = clock(); // Start timing
    
    int visited[MAX_NODES] = {0}; // Track visited nodes
    stats.memory_consumed += sizeof(int) * MAX_NODES; // Track memory
    // Initialize distances and parents
    for (int i = 0; i < MAX_NODES; i++) {
        dist[i] = INF;
        parent[i] = -1;
        segment_time[i] = 0;
    }
    dist[start] = 0; // Distance to source is 0
    
    // Process all nodes
    for (int count = 0; count < node_count; count++) {
        int u = -1; // Next node to process
        float min_dist = INF;
        
        // Find unvisited node with minimum distance
        for (int i = 0; i < node_count; i++) {
            stats.comparisons++;
            int node = nodes[i].node;
            if (!visited[node] && dist[node] < min_dist) {
                min_dist = dist[node];
                u = node;
            }
        }
        
        if (u == -1) break; // No more reachable nodes
        visited[u] = 1; // Mark node as visited
        
        // Process all edges from node u
        Edge* edge = adj[u];
        while (edge) {
            int v = edge->dest;
            float weight = edge->weight;
            
            stats.relaxations++;
            // Relax edge if shorter path found
            if (!visited[v] && dist[u] + weight < dist[v]) {
                stats.parent_changes++;
                dist[v] = dist[u] + weight;
                parent[v] = u;
                segment_time[v] = weight;
            }
            edge = edge->next;
        }
    }
    
    // Calculate execution time
    stats.exec_time = ((double)(clock() - start_time)) * 1000 / CLOCKS_PER_SEC;
}

// Function to write performance stats to CSV
void writePerformanceCSV(int startNode, int trial) {
    FILE* perf_file;
    int file_exists = 0;
    // Check if performance file exists
    perf_file = fopen("performance.csv", "r");
    if (perf_file) {
        file_exists = 1;
        fclose(perf_file);
    }

    // Open file in append or write mode
    perf_file = fopen("performance.csv", file_exists ? "a" : "w");
    if (!perf_file) {
        fprintf(stderr, "Error opening performance file\n");
        exit(EXIT_FAILURE);
    }

    // Write header if file is new
    if (!file_exists) {
        fprintf(perf_file, "Trial,Start Node,Execution Time (ms),Memory Consumed (bytes),Comparisons,Relaxations,Auxiliary Size (bytes),Parent Changes,Avg Parent Change\n");
    }

    // Calculate average parent changes
    float avg_parent_changes = stats.parent_changes > 0 ? 
                            (float)stats.relaxations / stats.parent_changes : 0;

    // Write stats to file
    fprintf(perf_file, "%d,%d,%.2f,%zu,%d,%d,%zu,%d,%.2f\n",
        trial, startNode, stats.exec_time, stats.memory_consumed, 
        stats.comparisons, stats.relaxations, stats.auxiliary_size,
        stats.parent_changes, avg_parent_changes);

    fclose(perf_file);
}

// Function to print path to a node recursively
void printPath(int node, FILE* out) {
    if (parent[node] == -1) { // Base case: source node
        fprintf(out, "[%d;0.00]", node);
        return;
    }
    printPath(parent[node], out); // Recursive call for parent
    fprintf(out, " -> [%d;%.2f]", node, segment_time[node]); // Print node and segment time
}

// Function to create a directory
void createDirectory(const char* path) {
    if (_mkdir(path) != 0 && errno != EEXIST) { // Create directory, handle errors
        perror("Error creating directory");
        exit(1);
    }
}

// Function to save shortest paths from a source node
void saveSingleSourcePaths(int source, int trial_num, const char* trial_dir) {
    // Create directory for source node
    char folder_name[256];
    sprintf(folder_name, "%s\\Node_%d", trial_dir, source);
    createDirectory(folder_name);
    
    // Create output file
    char filename[256];
    sprintf(filename, "%s\\paths_from_%d.csv", folder_name, source);
    
    dijkstra(source); // Run Dijkstra's algorithm
    
    // Open output file
    FILE* out = fopen(filename, "w");
    if (!out) {
        perror("Could not open output file");
        return;
    }

    fprintf(out, "Source,Destination,Path,TotalTime\n"); // Write header
    
    // Write paths to all reachable destinations
    for (int j = 0; j < node_count; j++) {
        int dest = nodes[j].node;
        if (source != dest && dist[dest] < INF) {
            fprintf(out, "%d,%d,", source, dest);
            printPath(dest, out);
            fprintf(out, ",%.2f\n", dist[dest]);
        }
    }
    fclose(out);
    
    writePerformanceCSV(source, trial_num); // Save performance stats
}

// Function to run a trial
void runTrial(int trial_num) {
    // Create trial directory
    char trial_dir[256];
    sprintf(trial_dir, "Trial_%d", trial_num);
    createDirectory(trial_dir);
    
    printf("Running trial %d...\n", trial_num);
    
    // Process each source node
    for (int i = 0; i < node_count; i++) {
        int source = nodes[i].node;
        saveSingleSourcePaths(source, trial_num, trial_dir);
        
        // Print progress every 10 nodes
        if ((i+1) % 10 == 0) {
            printf("  Processed %d/%d sources...\n", i+1, node_count);
        }
    }
    
    printf("Trial %d completed.\n", trial_num);
}

// Function to free graph memory
void freeGraph() {
    for (int i = 0; i < MAX_NODES; i++) {
        Edge* edge = adj[i];
        while (edge) { // Free all edges
            Edge* temp = edge;
            edge = edge->next;
            free(temp);
        }
        adj[i] = NULL;
    }
}

// Main function
int main() {
    // Initialize adjacency list
    for (int i = 0; i < MAX_NODES; i++) {
        adj[i] = NULL;
    }

    printf("Building adjacency list...\n");
    build_adjacency_list(); // Create adjacency list from CSV
    printf("Adjacency list built. Output written to berlin_list_bus.csv\n");

    printf("Loading graph for Dijkstra...\n");
    loadGraph("berlin_list_bus.csv"); // Load graph for Dijkstra
    printf("Found %d unique nodes\n", node_count);
    
    // Get number of trials from user
    int num_trials;
    printf("Enter the number of trials to perform: ");
    scanf("%d", &num_trials);
    
    // Run specified number of trials
    for (int trial = 1; trial <= num_trials; trial++) {
        runTrial(trial);
    }
    
    // Clean up memory
    freeGraph();
    free_route_memory();
    printf("All trials completed. Performance data saved to performance.csv\n");
    return 0;
}
