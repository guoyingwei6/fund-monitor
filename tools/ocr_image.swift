import Foundation
import Vision

guard CommandLine.arguments.count >= 2 else {
    FileHandle.standardError.write("usage: ocr_image <image-path>\n".data(using: .utf8)!)
    exit(2)
}

let imageURL = URL(fileURLWithPath: CommandLine.arguments[1])

let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
request.automaticallyDetectsLanguage = true
request.usesLanguageCorrection = true

let handler = VNImageRequestHandler(url: imageURL, options: [:])
do {
    try handler.perform([request])
} catch {
    FileHandle.standardError.write("ocr failed: \(error)\n".data(using: .utf8)!)
    exit(1)
}

let observations = request.results ?? []
let lines = observations.compactMap { observation in
    observation.topCandidates(1).first?.string
}
print(lines.joined(separator: "\n"))
