""" 
Partition Strategy for Egyptian Governorates

Divides 27 governorates into 3 partitions based on geographic regions:
- Partition 0: Northern Region (9 governorates)
- Partition 1: Central Region (9 governorates)
- Partition 2: Southern & Border Region (9 governorates)
"""

# Partition assignments based on geographic location
PARTITION_MAP = {
    # Partition 0: Northern Region (Delta and Mediterranean coast)
    0: [
        'Alexandria',
        'Beheira',
        'Kafr El Sheikh',
        'Dakahlia',
        'Damietta',
        'Port Said',
        'Gharbia',
        'Monufia',
        'Qalyubia'
    ],
    
    # Partition 1: Central Region (Greater Cairo and Upper Egypt North)
    1: [
        'Cairo',
        'Giza',
        'Faiyum',
        'Beni Suef',
        'Minya',
        'Asyut',
        'Sohag',
        'Qena',
        'Helwan'
    ],
    
    # Partition 2: Southern & Border Region (Far South and desert regions)
    2: [
        'Luxor',
        'Aswan',
        'Red Sea',
        'South Sinai',
        'North Sinai',
        'Ismailia',
        'Suez',
        'Matruh',
        'New Valley'
    ]
}

# Reverse mapping for quick lookup
GOVERNORATE_TO_PARTITION = {}
for partition_id, governorates in PARTITION_MAP.items():
    for gov in governorates:
        GOVERNORATE_TO_PARTITION[gov] = partition_id


def get_partition_for_governorate(governorate_name):
    """
    Returns the partition ID (0, 1, or 2) for a given governorate name.
    
    Args:
        governorate_name (str): Name of the governorate
        
    Returns:
        int: Partition ID (0, 1, or 2)
        
    Raises:
        ValueError: If governorate name is not found
    """
    partition = GOVERNORATE_TO_PARTITION.get(governorate_name)
    
    if partition is None:
        raise ValueError(
            f"Unknown governorate: {governorate_name}. "
            f"Valid governorates: {list(GOVERNORATE_TO_PARTITION.keys())}"
        )
    
    return partition


def get_governorates_by_partition(partition_id):
    """
    Returns list of governorates assigned to a specific partition.
    
    Args:
        partition_id (int): Partition ID (0, 1, or 2)
        
    Returns:
        list: List of governorate names
        
    Raises:
        ValueError: If partition_id is invalid
    """
    if partition_id not in PARTITION_MAP:
        raise ValueError(f"Invalid partition ID: {partition_id}. Must be 0, 1, or 2")
    
    return PARTITION_MAP[partition_id]


def get_partition_info():
    """
    Returns complete information about all partitions.
    
    Returns:
        dict: Dictionary with partition details
    """
    return {
        'total_partitions': len(PARTITION_MAP),
        'total_governorates': sum(len(govs) for govs in PARTITION_MAP.values()),
        'partitions': {
            0: {
                'name': 'Northern Region',
                'description': 'Delta and Mediterranean coast',
                'governorates': PARTITION_MAP[0],
                'count': len(PARTITION_MAP[0])
            },
            1: {
                'name': 'Central Region',
                'description': 'Greater Cairo and Upper Egypt North',
                'governorates': PARTITION_MAP[1],
                'count': len(PARTITION_MAP[1])
            },
            2: {
                'name': 'Southern & Border Region',
                'description': 'Far South and desert regions',
                'governorates': PARTITION_MAP[2],
                'count': len(PARTITION_MAP[2])
            }
        }
    }


if __name__ == '__main__':
    # Print partition information
    info = get_partition_info()
    print(f"\nEgyptian Governorates Partition Strategy")
    print(f"Total Partitions: {info['total_partitions']}")
    print(f"Total Governorates: {info['total_governorates']}\n")
    
    for partition_id, details in info['partitions'].items():
        print(f"Partition {partition_id}: {details['name']}")
        print(f"  Description: {details['description']}")
        print(f"  Count: {details['count']}")
        print(f"  Governorates: {', '.join(details['governorates'])}")
        print()
