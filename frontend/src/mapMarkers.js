import L from "leaflet";

function createCircleIcon({ fillColor, size, strokeColor = "#fff", strokeWidth = 1.5 }) {
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" viewBox="0 0 32 32" aria-hidden="true">
      <circle cx="16" cy="16" r="13" fill="${fillColor}" stroke="${strokeColor}" stroke-width="${strokeWidth}" />
    </svg>
  `;

  return L.divIcon({
    className: "map-circle-marker",
    html: svg,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor: [0, -size / 2],
    tooltipAnchor: [0, -size / 2],
  });
}

export function createCircleMarker(latlng, {
  fillColor,
  selectedFillColor = fillColor,
  size = 14,
  selectedSize = size + 2,
  strokeColor = "#fff",
  strokeWidth = 1.5,
  ...options
} = {}) {
  const defaultIcon = createCircleIcon({ fillColor, size, strokeColor, strokeWidth });
  const selectedIcon = createCircleIcon({ fillColor: selectedFillColor, size: selectedSize, strokeColor, strokeWidth });
  const marker = L.marker(latlng, {
    icon: defaultIcon,
    keyboard: true,
    ...options,
  });

  marker.defaultIcon = defaultIcon;
  marker.selectedIcon = selectedIcon;

  marker.setStyle = (style = {}) => {
    const color = style.fillColor || fillColor;
    const nextSize = Number(style.size ?? (style.radius ? style.radius * 2.2 : size));
    const icon = createCircleIcon({
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
