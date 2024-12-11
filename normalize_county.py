def normalize_county_name(county: str) -> str:
    """
    Normalize county names by removing common suffixes and standardizing 'Saint' to 'ST'.
    
    Args:
        county: Input county name (str)
        
    Returns:
        Normalized county name (str)
    """
    # Convert to uppercase for consistent processing
    county = county.upper().strip()
    
    # Standardize 'Saint' to 'ST'
    saint_variants = ["SAINT", "ST.", "STE.", "ST"]
    for variant in saint_variants:
        if county.startswith(variant):
            county = county.replace(variant, "ST", 1)
            break
    
    # Remove common suffixes
    suffixes = [
        " COUNTY",
        " PARISH",
        " BOROUGH",
        " CITY",
        " AREA",
        " MUNICIPALITY",
        " DISTRICT"
    ]
    
    # Remove any suffix
    for suffix in suffixes:
        if county.endswith(suffix):
            county = county[:-len(suffix)]
    
    return county

# Example usage and tests
def test_county_normalizer():
    test_cases = [
        "Saint Louis County",
        "St. Charles Parish",
        "Ste. Genevieve Borough",
        "St Louis City",
        "Orleans Parish",
        "New York Borough",
        "King County",
        "Jefferson Area"
    ]
    
    print("Test Results:")
    print("-" * 40)
    for test in test_cases:
        normalized = normalize_county_name(test)
        print(f"Original: {test}")
        print(f"Normalized: {normalized}")
        print("-" * 40)

if __name__ == "__main__":
    test_county_normalizer()