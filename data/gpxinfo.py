import xml.etree.ElementTree as ET


def count_points_gpx(archivo_gpx):
    tree = ET.parse(archivo_gpx)
    root = tree.getroot()

    ns = {'gpx': 'http://www.topografix.com/GPX/1/1'}

    track_points = root.findall('.//gpx:trk//gpx:trkseg//gpx:trkpt', ns)

    total_points = len(track_points)
    return total_points


total_points = count_points_gpx('2022-12-02 12_12_41.gpx')
print(f"Total measured points into GPX file: {total_points}")
