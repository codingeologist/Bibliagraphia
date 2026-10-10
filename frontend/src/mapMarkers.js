import L from "leaflet";

const STAR_PATH = "M16 2 L19.8 10.6 L29 11.3 L22 17.5 L24.8 27.1 L16 21.9 L7.2 27.1 L10 17.5 L3 11.3 L12.2 10.6 Z";

function createStarIcon({ fillColor, size, strokeColor = "#fff", strokeWidth = 1.5 }) {
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 32 32" aria-hidden="true">
      <path d="${STAR_PATH}" fill="${fillColor}" stroke="${strokeColor}" stroke-width="${strokeWidth}" stroke-linejoin="round" />
    </svg>
  `;

  return L.divIcon({
    className: "map-star-marker",
    html: svg,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor: [0, -size / 2],
    tooltipAnchor: [0, -size / 2],
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
    const color = style.fillColor || fillColor;
    const nextSize = Number(style.size ?? style.radius ? style.radius * 2.2 : size);
    const icon = createStarIcon({
      fillColor: color,
      size: Math.max(12, nextSize),
      strokeColor,
      strokeWidth,
    });
    marker.setIcon(icon);
    marker.defaultIcon = icon;
  };

  marker.setSelected = (selected) => {
    marker.setIcon(selected ? selectedIcon : defaultIcon);
  };

  return marker;
}
