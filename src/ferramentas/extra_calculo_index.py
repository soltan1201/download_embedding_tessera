import ee
import os
import sys


projAccount = "ee-solkancengine17"

try:
    ee.Initialize(project=projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error: {e}")
    sys.exit(1)

endmembers = {
    'landsat-4': [
        [119.0, 475.0, 169.0, 6250.0, 2399.0, 675.0],         #/*gv*/
        [1514.0, 1597.0, 1421.0, 3053.0, 7707.0, 1975.0],         #/*npv*/
        [1799.0, 2479.0, 3158.0, 5437.0, 7707.0, 6646.0],         #/*soil*/
        [4031.0, 8714.0, 7900.0, 8989.0, 7002.0, 6607.0]         #/*cloud*/
    ],
    'landsat-5': [
        [119.0, 475.0, 169.0, 6250.0, 2399.0, 675.0],         #/*gv*/
        [1514.0, 1597.0, 1421.0, 3053.0, 7707.0, 1975.0],         #/*npv*/
        [1799.0, 2479.0, 3158.0, 5437.0, 7707.0, 6646.0],         #/*soil*/
        [4031.0, 8714.0, 7900.0, 8989.0, 7002.0, 6607.0]         #/*cloud*/
    ],
    'landsat-7': [
        [119.0, 475.0, 169.0, 6250.0, 2399.0, 675.0],         #/*gv*/
        [1514.0, 1597.0, 1421.0, 3053.0, 7707.0, 1975.0],         #/*npv*/
        [1799.0, 2479.0, 3158.0, 5437.0, 7707.0, 6646.0],         #/*soil*/
        [4031.0, 8714.0, 7900.0, 8989.0, 7002.0, 6607.0]         #/*cloud*/
    ],
    'landsat-8': [
        [119.0, 475.0, 169.0, 6250.0, 2399.0, 675.0],         #/*gv*/
        [1514.0, 1597.0, 1421.0, 3053.0, 7707.0, 1975.0],         #/*npv*/
        [1799.0, 2479.0, 3158.0, 5437.0, 7707.0, 6646.0],         #/*soil*/
        [4031.0, 8714.0, 7900.0, 8989.0, 7002.0, 6607.0]         #/*cloud*/
    ],
    'sentinel-2': [
        [119.0, 475.0, 169.0, 6250.0, 2399.0, 675.0],         #/*gv*/
        [1514.0, 1597.0, 1421.0, 3053.0, 7707.0, 1975.0],         #/*npv*/
        [1799.0, 2479.0, 3158.0, 5437.0, 7707.0, 6646.0],         #/*soil*/
        [4031.0, 8714.0, 7900.0, 8989.0, 7002.0, 6607.0]         #/*cloud*/
    ],
}


def getFractions_and_index (image, endmembers):

    outBandNames = ['gv', 'npv', 'soil', 'cloud']

    fractions = (ee.Image(image)
                    .select(['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])
                    .unmix(endmembers)
                    .max(0)
                    .multiply(100)
                    # .byte()
    )
    fractions = fractions.rename(outBandNames)
    summed = fractions.expression('b("gv") + b("npv") + b("soil")')

    shade = summed.subtract(100).abs().byte().rename("shade")

    gvs = (fractions.select("gv")
                    .divide(summed)
                    .multiply(100)
                    .byte()
                    .rename("gvs"))

    npvSoil = fractions.expression('b("npv") + b("soil")')
    ndfi = (ee.Image.cat(gvs, npvSoil)
                    .normalizedDifference()
                    .rename('ndfi')
                )
    # // rescale NDFI from 0 to 200
    ndfi = ndfi.expression('byte(b("ndfi") * 100 + 100)')
    gvnpv_s = (fractions.select("gv")
                    .add(fractions.select("npv"))
                    .divide(summed).multiply(100)
                    )
    # //calculate SEFI
    sefi = (ee.Image.cat(gvnpv_s, fractions.select("soil"))
                .normalizedDifference()
                .rename('sefi')
            )
    # // rescale SEFI from 0 to 200
    sefi = sefi.expression('byte(b("sefi") * 100 + 100)')
    shd = summed.subtract(100).abs()# .byte()
    gvnpv = fractions.select("gv").add(fractions.select("npv"))
    soilshade = fractions.select("soil").add(shd)
    # //calculate WEFI
    wefi = (ee.Image.cat(gvnpv, soilshade)
                        .normalizedDifference()
                        .rename('wefi'))

    # // rescale WEFI from 0 to 200
    wefi = wefi.expression('byte(b("wefi") * 100 + 100)')
    
    image = image.addBands(fractions)
    image = image.addBands(shade)
    image = image.addBands(gvs)
    image = image.addBands(ndfi)
    image = image.addBands(wefi)
    image = image.addBands(sefi)

    return image




#   collection = collection
#       .map(ind.getCAI)
#       .map(ind.getEVI2)
#       .map(ind.getGCVI)
#       .map(ind.getHallCover)
#       .map(ind.getHallHeigth)
#       .map(ind.getNDVI)
#       .map(ind.getNDWI)
#       .map(ind.getPRI)
#       .map(ind.getSAVI);


def get_indexs_spectral(image):

    exp = '( b("nir") - b("red") ) / ( b("nir") + b("red") )'

    ndvi = (image.expression(exp).rename("ndvi")
		            .add(1).multiply(1000).int16()
    )
    exp = 'float(b("nir") - b("swir1"))/(b("nir") + b("swir1"))';

    ndwi = (image.expression(exp).rename("ndwi")
		        .add(1).multiply(1000).int16())

    exp = '1.5 * (b("nir") - b("red")) / (0.5 + b("nir") + b("red"))'
    savi = (image.expression(exp).rename("savi")
		        .add(1).multiply(1000).int16())