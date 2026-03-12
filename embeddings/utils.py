
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt

from shapely.geometry import box

def make_bounding_box(df: gpd.GeoDataFrame, margin_degrees: float = 1.0):
    minx, miny, maxx, maxy = df.geometry.total_bounds
    df_box = box(
        minx=minx - margin_degrees,
        miny=miny - margin_degrees,
        maxx=maxx + margin_degrees,
        maxy=maxy + margin_degrees,
    )
    df_box = gpd.GeoDataFrame([{"geometry": df_box}], crs="EPSG:4326")
    return df_box

def _dilate_mask(mask: np.ndarray, iterations: int = 1) -> np.ndarray:
    """Pure-numpy 3x3 binary dilation (boolean mask)."""
    if iterations <= 0:
        return mask.copy()
    m = mask.copy()
    H, W = m.shape
    for _ in range(iterations):
        p = np.pad(m, pad_width=1, mode='constant', constant_values=False)
        new = np.zeros_like(m, dtype=bool)
        for dx in (0, 1, 2):
            for dy in (0, 1, 2):
                new |= p[dx:dx+H, dy:dy+W]
        m = new
    return m

def array_to_rgb_imshow(arr: np.ndarray,
                        expand: bool = False,
                        iterations: int = 1,
                        overlap: str = 'add',
                        show: bool = True) -> np.ndarray:
    """
    Convert (H, W, 2) float array -> (H, W, 3) uint8 RGB.
    Any positive value in channel 0 -> yellow ([255,255,0]).
    Any positive value in channel 1 -> green  ([0,255,0]).
    
    Parameters
    ----------
    arr : np.ndarray
        Input array with shape (H, W, 2).
    expand : bool
        If True, dilate positive regions for both channels.
    iterations : int
        Dilation iterations when expand=True.
    overlap : {'add', 'priority0', 'priority1'}
        How to handle pixels where both channels are positive.
    show : bool
        If True, show the resulting image with matplotlib.pyplot.imshow.
    
    Returns
    -------
    rgb_uint8 : np.ndarray
        uint8 image with shape (H, W, 3), values 0..255.
    """
    if not isinstance(arr, np.ndarray):
        raise TypeError("arr must be a numpy array")
    if arr.ndim != 3 or arr.shape[2] != 2:
        raise ValueError("arr must have shape (H, W, 2)")
    if overlap not in ('add', 'priority0', 'priority1'):
        raise ValueError("overlap must be 'add', 'priority0' or 'priority1'")

    H, W, _ = arr.shape
    a = arr  # no scaling needed

    mask0 = a[..., 0] > 0
    mask1 = a[..., 1] > 0

    if expand:
        mask0 = _dilate_mask(mask0, iterations=iterations)
        mask1 = _dilate_mask(mask1, iterations=iterations)

    # Work in uint16 to avoid overflow when adding, then clip to 0..255.
    rgb = np.zeros((H, W, 3), dtype=np.uint16)

    YELLOW = np.array([255, 255,   0], dtype=np.uint16)
    GREEN  = np.array([  0, 255,   0], dtype=np.uint16)

    if overlap == 'add':
        # Add contributions, clip later
        rgb[mask0] += YELLOW
        rgb[mask1] += GREEN
    elif overlap == 'priority0':
        # channel0 wins where both true
        both = mask0 & mask1
        only1 = mask1 & ~mask0
        rgb[mask0] = YELLOW
        rgb[only1] = GREEN
        # both already covered by mask0 (yellow)
    else:  # 'priority1'
        both = mask0 & mask1
        only0 = mask0 & ~mask1
        rgb[mask1] = GREEN
        rgb[only0] = YELLOW
        # both covered by mask1 (green)

    # Clip to 0..255 and convert to uint8
    rgb = np.clip(rgb, 0, 255).astype(np.uint8)

    if show:
        plt.figure(figsize=(5, 5))
        plt.imshow(rgb, interpolation='nearest')
        plt.axis('off')
        plt.show()

    return rgb