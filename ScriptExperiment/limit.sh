#!/bin/bash

# Define your target nodes array
target_nodes=($(uniq "$OAR_NODEFILE" | awk -F'.' '{print $1}'))
echo "target_nodes"
echo "$target_nodes"
bandwidth_limits=( 1gbit )  # Add more limits as needed
#bandwidth_limits=(40mbit)
configured_pairs=()  # Array to keep track of configured pairs
#bandwidth_limits=(80mbit)
# Initialize the index for bandwidth limits
limit_index=0

# Clear existing traffic control rules (optional, if you want a clean start)
for node in "${target_nodes[@]}"; do
    # Determine the network interface based on the node type
    if [[ "$node" == *"parasilo"* ]]; then
        interface="eno1"
    elif [[ "$node" == *"paradoxe"* ]]; then
	echo "aa"
        interface="ens10f0np0"
    elif [[ "$node" == *"dahu"* ]]; then
	echo "aa"
        interface="enp24s0f0"
    else
        echo "skip"
        continue
    fi

    # Clear existing traffic control rules
    ssh root@"$node" "sudo tc qdisc del dev $interface root 2> /dev/null"
done

# Set up traffic control on all pairs of nodes
for i in "${!target_nodes[@]}"; do
    for j in "${!target_nodes[@]}"; do
        if [ "$i" -ne "$j" ]; then
	echo "enterrrr"
            node1="${target_nodes[$i]}"
            node2="${target_nodes[$j]}"
	   echo "$node1 .. $node2"
            # Sort nodes to create a unique identifier (to avoid order issues)
            sorted_pair=($(echo -e "$node1\n$node2" | sort))  # Sort the pair
            pair="${sorted_pair[0]}-${sorted_pair[1]}"  # Create a unique identifier for the sorted pair

            # Check if this pair has already been configured
            if [[ " ${configured_pairs[@]} " =~ " ${pair} " ]]; then
                echo "Bandwidth already configured between $node1 and $node2 (or vice versa). Skipping..."
                continue
            fi

            # Determine the network interface for the nodes
            if [[ "$node1" == *"parasilo"* ]]; then
                interface1="eno1"
            elif [[ "$node1" == *"paradoxe"* ]]; then
                interface1="ens10f0np0"
            elif [[ "$node" == *"dahu"* ]]; then
                interface1="enp24s0f0"
            else
                echo "skip"
                continue
            fi

            if [[ "$node2" == *"parasilo"* ]]; then
                interface2="eno1"
            elif [[ "$node2" == *"paradoxe"* ]]; then
                interface2="ens10f0np0"
            elif [[ "$node" == *"dahu"* ]]; then
                interface2="enp24s0f0"
            else
                echo "skip"
                continue
            fi
            echo "comeeee"
            # Get the bandwidth limit for the current pair of nodes
            bandwidth_limit="${bandwidth_limits[$limit_index]}"

            # Check if the qdisc exists; if not, add it
            ssh root@$node1 "sudo tc qdisc show dev $interface1 | grep -q 'htb' || sudo tc qdisc add dev $interface1 root handle 1: htb default 11"
            ssh root@$node2 "sudo tc qdisc show dev $interface2 | grep -q 'htb' || sudo tc qdisc add dev $interface2 root handle 1: htb default 11"

            # Define a unique class ID for each pair of nodes
            class_id="1:$((i + j))"

            # Add class to limit bandwidth (if it doesn't exist)
            ssh root@$node1 "sudo tc class show dev $interface1 | grep -q \"classid $class_id\" || sudo tc class add dev $interface1 parent 1: classid $class_id htb rate $bandwidth_limit ceil $bandwidth_limit"
            ssh root@$node2 "sudo tc class show dev $interface2 | grep -q \"classid $class_id\" || sudo tc class add dev $interface2 parent 1: classid $class_id htb rate $bandwidth_limit ceil $bandwidth_limit"

            # Add filter to Node 1 for Node 2
            ssh root@$node1 "sudo tc filter show dev $interface1 protocol ip parent 1: | grep -q \"dst \$(getent hosts $node2 | awk '{ print \$1 }')\" || sudo tc filter add dev $interface1 protocol ip parent 1: prio 1 u32 match ip dst \$(getent hosts $node2 | awk '{ print \$1 }') flowid $class_id"

            # Add filter to Node 2 for Node 1
            ssh root@$node2 "sudo tc filter show dev $interface2 protocol ip parent 1: | grep -q \"dst \$(getent hosts $node1 | awk '{ print \$1 }')\" || sudo tc filter add dev $interface2 protocol ip parent 1: prio 1 u32 match ip dst \$(getent hosts $node1 | awk '{ print \$1 }') flowid $class_id"

            echo "Limiting bandwidth between $node1 and $node2 to $bandwidth_limit"

            # Add the pair to the configured pairs list
            configured_pairs+=("$pair")

            # Increment the limit index and wrap around if it exceeds the array size
            limit_index=$(( (limit_index + 1) % ${#bandwidth_limits[@]} ))
        fi
    done
done

echo "Bandwidth limiting setup completed."

