cd ./test

if [ $# -ne 2 ]; then
    echo "Usage: $0 <number_of_clients> <expected_results_file_name_not_path>"
    echo "Example: $0 1 expected_results.json"
    echo "Example: $0 4 expected_results_10k.json"
    exit 1
fi

N_CLIENTS=$1
EXPECTED_FILE=$2

ls ${EXPECTED_FILE} &> /dev/null

if [ $? -ne 0 ]; then
    echo "Expected results not found..."
    echo "Running generator to create expected results"
    bash ../generate_expected.sh
fi

echo "Running comparisons"

for i in $(seq 0 $((N_CLIENTS - 1))); do
    echo ""
    ls ./actual_results_client_${i}.json &> /dev/null
    
    if [ $? -ne 0 ]; then
        echo "Actual results for client_${i} not found..."
        echo "Run docker-compose up with CLIENT=${N_CLIENTS} in infra/config.ini (if you edit config, generate compose again) to generate actual results to all clients"
        exit 1
    fi

    echo "Comparing actual results for client_${i}"
    python3 compare_results.py ${EXPECTED_FILE} ./actual_results_client_${i}.json
done

echo "All actual results found"

cd ..