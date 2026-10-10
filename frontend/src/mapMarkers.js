import L from "leaflet";

const STAR_PATH = "M 0 -10 L 2.9 -3.2 L 10 -3.2 L 4.4 0.8 L 6.5 8.5 L 0 3.7 L -6.5 8.5 L -4.4 0.8 L -10 -3.2 L -2.9 -3.2 Z";

function createStarIcon({ fillColor, size, strokeColor = "#fff", strokeWidth = 1.5 }) {
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="-12 -12 24 24" aria-hidden="true">
      <path d="${STAR_PATH}" fill="${fillColor}" stroke="${strokeColor}" stroke-width="${strokeWidth}" stroke-linejoin="round" />
    </svg>
  `;

  return L.divIcon({
    className: "map-star-marker",
    html: svg,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor: [0, -size / 2],
  });
}

export function createStarMarker(latlng, {
  fillColor,
  selectedFillColor = fillColor,
  size = 14,
  selectedSize = size + 2,
  strokeColor = "#fff",
  strokeWidth = 1.5,
  ...options
} = {}) {
  const defaultIcon = createStarIcon({ fillColor, size, strokeColor, strokeWidth });
  const selectedIcon = createStarIcon({ fillColor: selectedFillColor, size: selectedSize, strokeColor, strokeWidth });
  const marker = L.marker(latlng, {
    icon: defaultIcon,
    keyboard: true,
    ...options,
  });

  marker.defaultIcon = defaultIcon;
  marker.selectedIcon = selectedIcon;

  marker.setStyle = (style = {}) => {
    const radius = Number(style.radius ?? size);
    const color = style.fillColor || fillColor;
    const nextSize = Math.max(12, radius * 2.2);
    marker.setIcon(createStarIcon({ fillColor: color, size: nextSize, strokeColor, strokeWidth }));
  };

  marker.setSelected = (selected) => {
    marker.setIcon(selected ? selectedIcon : defaultIcon);
  };

  return marker;
}
